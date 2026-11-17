# Databricks notebook source
# MAGIC %md
# MAGIC # ForaDoPico · 04 · Integração com o RPA
# MAGIC
# MAGIC Recebe os lotes de escala do **robô RPA** (planilha do RH validada e pseudonimizada) e transforma em escala sugerida
# MAGIC pelo ForaDoPico, usando a lotação prevista da camada Ouro.
# MAGIC
# MAGIC | Passo | Seção | O que cria |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Bronze | `bronze_schedule`: o lote JSON bruto, um registro por arquivo (Auto Loader incremental) |
# MAGIC | 2 | Prata | `prata_lote_escala`, `prata_demanda_time`, `prata_funcionario_semana` e `prata_escala_atual`, com regras de qualidade e MERGE |
# MAGIC | 3 | Ouro | escala sugerida, `fato_escala_rpa` (atual × sugerida), aderência à demanda do RH e pendências |
# MAGIC | 4 | Consulta 6 | impacto da escala sugerida nos dados reais do RH |
# MAGIC | 5 | Uso pelo MVP | view `vw_app_escala_rpa` |
# MAGIC
# MAGIC **Contrato com o RPA** (documento `landingZoneHandoff.md`, versão do formato 2): um arquivo `scheduleAAAAMMDD_HHMMSS_<hash8>.json`
# MAGIC por planilha aceita, na pasta `chegada/schedule/`. O robô grava um `.tmp` e renomeia; aqui só os `.json` são lidos.
# MAGIC O funcionário chega como `employeeRef` (SHA-256 da matrícula com salt, 12 caracteres): **sem nome e sem e-mail**.
# MAGIC
# MAGIC **Pré-requisito:** ter rodado o `01_bronze`, o `02_prata` e o `03_ouro` (este notebook usa `prata_estacao`, `dim_data`,
# MAGIC `dim_faixa_horaria` e `ouro_lotacao_prevista`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuração
# MAGIC Cria a pasta `chegada/schedule/` no Volume e o campo **etapa**: `1_lote_w47`, `2_lote_w48` e `3_reexecucao`.

# COMMAND ----------

import hashlib
import math
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "workspace"
SCHEMA = "foradopico"
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada"
CONTROLE = f"/Volumes/{CATALOGO}/{SCHEMA}/controle"
PASTA_RPA = f"{VOLUME}/schedule"

TEMPO_VIAGEM_MIN = 60     # mesma hipótese do 03_ouro: embarque 1 hora antes da entrada
LIMITE_POR_JANELA = 0.40  # no máximo 40% das pessoas do time no dia na mesma janela

ATUAL = "Atual (RH)"
SUGERIDA = "Sugerida (ForaDoPico)"


def t(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def salvar(df, nome: str) -> None:
    """A Ouro é recalculada inteira a partir da Prata a cada execução."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(nome))
    print(f"{nome}: {spark.table(t(nome)).count()} linhas")


def minutos(coluna_hhmm: str):
    return F.expr(f"try_cast(substr({coluna_hhmm}, 1, 2) AS INT) * 60 + try_cast(substr({coluna_hhmm}, 4, 2) AS INT)")


spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
dbutils.fs.mkdirs(PASTA_RPA)
dbutils.widgets.text("etapa", "1_lote_w47", "Etapa (1_lote_w47, 2_lote_w48, 3_reexecucao)")
EXECUCAO = spark.sql("SELECT current_timestamp() AS agora").first().agora
print("Pronto. Pasta dos lotes do robô:", PASTA_RPA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: o lote como chegou
# MAGIC O Auto Loader lê cada arquivo **inteiro** (formato `binaryFile`) e guarda o JSON original em texto. Assim a Bronze
# MAGIC aceita qualquer versão do formato, e a interpretação fica toda na Prata. O filtro `schedule*.json` ignora os `.tmp`
# MAGIC que o robô usa durante a gravação. O checkpoint garante que cada arquivo entra uma única vez.

# COMMAND ----------

consulta = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_schedule")
    .option("pathGlobFilter", "schedule*.json")
    .load(PASTA_RPA)
    .select(
        F.col("path").alias("arquivo_origem"),
        F.col("length").alias("tamanho_bytes"),
        F.decode(F.col("content"), "UTF-8").alias("conteudo_json"),
        F.current_timestamp().alias("carregado_em"),
    )
    .writeStream.option("checkpointLocation", f"{CONTROLE}/checkpoint_schedule")
    .trigger(availableNow=True)
    .toTable(t("bronze_schedule"))
)
consulta.awaitTermination()

etapa = dbutils.widgets.get("etapa")
r = spark.sql(f"SELECT count(*) AS registros, count(DISTINCT arquivo_origem) AS arquivos FROM {t('bronze_schedule')}").first()
(
    spark.createDataFrame([(f"rpa_{etapa}", "bronze_schedule", r.registros, r.arquivos)],
                          "etapa string, tabela string, registros long, arquivos long")
    .withColumn("medido_em", F.current_timestamp())
    .write.mode("append").saveAsTable(t("controle_contagens_bronze"))
)

display(spark.sql(f"""
    SELECT etapa, tabela, registros, arquivos, medido_em
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY etapa ORDER BY medido_em DESC) AS ordem
        FROM {t('controle_contagens_bronze')}
        WHERE tabela = 'bronze_schedule'
    )
    WHERE ordem = 1
    ORDER BY etapa
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Prova de que nenhum lote entrou duas vezes

# COMMAND ----------

display(spark.sql(f"""
    SELECT regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo, tamanho_bytes,
           COUNT(*) AS registros, COUNT(DISTINCT carregado_em) AS cargas, min(carregado_em) AS carregado_em
    FROM {t('bronze_schedule')}
    GROUP BY arquivo_origem, tamanho_bytes
    ORDER BY arquivo
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prata: lote interpretado, validado e com MERGE
# MAGIC - O JSON é lido com um **schema explícito** da versão 2. Lotes da versão 1 também entram: os campos novos (`plannedDays`)
# MAGIC   ficam vazios.
# MAGIC - **Lote vigente:** se o RH reenviar a planilha da mesma semana, vale o lote mais recente (`generatedAt`).
# MAGIC   O MERGE substitui as linhas daquela semana e apaga as que não vieram no lote novo.
# MAGIC - Cada regra de qualidade conta o total barrado em `controle_qualidade_prata`, e os registros barrados vão para `quarentena_rpa`.

# COMMAND ----------

ESQUEMA = """
    dataset STRING, schemaVersion INT, batchId STRING, generatedAt STRING, sourceFileHash STRING,
    importId STRING, scheduleId STRING, companyName STRING, week STRING, entryWindows ARRAY<STRING>,
    teams ARRAY<STRUCT<teamName: STRING,
        demand: ARRAY<STRUCT<date: STRING, minPeople: INT, maxPeople: INT, mandatory: BOOLEAN>>>>,
    employees ARRAY<STRUCT<employeeRef: STRING, teamName: STRING, lineNumber: INT, boardingStation: STRING,
        monthlyOnSiteDays: INT, unavailableDates: ARRAY<STRING>, latestEntryWindow: STRING,
        plannedDays: ARRAY<STRUCT<date: STRING, entryWindow: STRING>>>>,
    quality STRUCT<employeesTotal: INT, employeesValid: INT, demandTotal: INT, demandValid: INT, rejectedRows: INT,
        currentScheduleTotal: INT, currentScheduleValid: INT>
"""

qualidade, quarentena, merges, tratamentos = [], [], [], []


def chave_nome(coluna):
    """Nome sem acento, minúsculo e só com letras e números (o mesmo do 02_prata)."""
    return F.regexp_replace(
        F.translate(F.lower(F.trim(coluna)), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"),
        "[^a-z0-9]",
        "",
    )


def aplicar_regras(df, tabela, regras):
    """Cada registro é barrado pela primeira regra que violar e vai para a quarentena; o restante segue."""
    avaliados = df.count()
    restante = df
    for codigo, descricao, invalido in regras:
        condicao = F.coalesce(invalido, F.lit(True))
        barrados = restante.filter(condicao)
        qualidade.append((tabela, codigo, descricao, avaliados, barrados.count()))
        colunas = [c for c in barrados.columns if not c.startswith("_")]
        quarentena.append(barrados.select(
            F.lit(tabela).alias("tabela"), F.lit(codigo).alias("regra"),
            F.to_json(F.struct(*colunas)).alias("registro"), F.col("arquivo_origem"),
        ))
        restante = restante.filter(~condicao)
    return restante


def com_hash(df, colunas_negocio):
    valores = [F.coalesce(F.col(c).cast("string"), F.lit("<nulo>")) for c in colunas_negocio]
    return df.withColumn("hash_registro", F.sha2(F.concat_ws("||", *valores), 256)).withColumn(
        "processado_em", F.current_timestamp())


def merge_rpa(df, tabela, chaves, semanas=None):
    """Upsert pela chave. Com `semanas`, apaga as linhas dessas semanas que não vieram no lote vigente."""
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(t(tabela))
    df.createOrReplaceTempView("lote_rpa")
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    apagar = ""
    if semanas:
        lista = ", ".join(f"'{s}'" for s in semanas)
        apagar = f"WHEN NOT MATCHED BY SOURCE AND alvo.semana IN ({lista}) THEN DELETE"
    r = spark.sql(f"""
        MERGE INTO {t(tabela)} AS alvo
        USING lote_rpa AS novo
        ON {condicao}
        WHEN MATCHED AND alvo.hash_registro <> novo.hash_registro THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        {apagar}
    """).first()
    ins, upd, dele = int(r["num_inserted_rows"]), int(r["num_updated_rows"]), int(r["num_deleted_rows"])
    merges.append((tabela, df.count(), ins, upd, dele, spark.table(t(tabela)).count()))
    print(f"{tabela}: {ins} inseridos, {upd} atualizados, {dele} removidos")


def fora_da_semana(coluna_data):
    """A data não pertence à semana ISO do lote (ex.: 2026-W48 = 23 a 29/11)."""
    return F.col(coluna_data).isNull() | (
        F.format_string("%d-W%02d", F.expr(f"extract(YEAROFWEEK FROM {coluna_data})"), F.weekofyear(coluna_data))
        != F.col("semana")
    )


print("Funções prontas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 `prata_lote_escala`: um registro por lote, com a marca do lote vigente da semana

# COMMAND ----------

lotes = spark.table(t("bronze_schedule")).select(
    "arquivo_origem", "carregado_em", F.from_json("conteudo_json", ESQUEMA).alias("j"))

lotes = aplicar_regras(lotes, "prata_lote_escala", [
    ("L1", "Arquivo não é um lote de escala (JSON inválido ou dataset diferente de 'schedule')",
     F.col("j.dataset").isNull() | (F.col("j.dataset") != "schedule")),
    ("L2", "Versão do formato não suportada (aceitas: 1 e 2)", ~F.col("j.schemaVersion").isin(1, 2)),
    ("L3", "Lote sem identificação (batchId ausente ou semana fora do padrão AAAA-Wnn)",
     F.col("j.batchId").isNull() | ~F.col("j.week").rlike("^[0-9]{4}-W[0-9]{2}$")),
])

# o mesmo lote (batchId) nunca entra duas vezes; se aparecer em dois arquivos, fica o mais recente
lotes = (
    lotes.withColumn("batch_id", F.col("j.batchId"))
    .withColumn("semana", F.col("j.week"))
    .withColumn("_empresa", F.col("j.companyName"))
    .withColumn("gerado_em", F.expr("try_cast(j.generatedAt AS TIMESTAMP)"))
    .withColumn("_ordem", F.row_number().over(
        Window.partitionBy("batch_id").orderBy(F.col("arquivo_origem").desc())))
    .filter("_ordem = 1").drop("_ordem")
    .withColumn("vigente", F.row_number().over(
        Window.partitionBy("_empresa", "semana").orderBy(F.col("gerado_em").desc(), F.col("batch_id").desc())) == 1)
)

lote_escala = com_hash(
    lotes.select(
        "batch_id", "semana",
        F.col("j.companyName").alias("empresa_nome"),
        F.col("j.schemaVersion").alias("versao_formato"),
        "gerado_em",
        F.col("j.sourceFileHash").alias("hash_planilha"),
        F.col("j.importId").alias("import_id"),
        F.col("j.scheduleId").alias("schedule_id"),
        F.col("j.entryWindows").alias("janelas_permitidas"),
        F.col("j.quality.employeesTotal").alias("funcionarios_na_planilha"),
        F.col("j.quality.employeesValid").alias("funcionarios_aceitos"),
        F.col("j.quality.rejectedRows").alias("linhas_recusadas_pelo_robo"),
        F.col("j.quality.currentScheduleValid").alias("linhas_escala_atual"),
        "vigente", "arquivo_origem",
    ),
    ["semana", "empresa_nome", "versao_formato", "gerado_em", "hash_planilha", "janelas_permitidas", "vigente"],
)
merge_rpa(lote_escala, "prata_lote_escala", ["batch_id"])

vigentes = lotes.filter("vigente")
SEMANAS = sorted(r.semana for r in vigentes.select("semana").distinct().collect())
print("Semanas com lote vigente:", SEMANAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 `prata_demanda_time`: quantas pessoas cada time precisa por dia

# COMMAND ----------

demanda = (
    vigentes.select("batch_id", "semana", "arquivo_origem", F.explode("j.teams").alias("tm"))
    .select("batch_id", "semana", "arquivo_origem", F.col("tm.teamName").alias("time_nome"),
            F.explode("tm.demand").alias("d"))
    .select(
        "semana", "time_nome",
        F.expr("try_cast(d.date AS DATE)").alias("data"),
        F.col("d.minPeople").alias("min_pessoas"),
        F.col("d.maxPeople").alias("max_pessoas"),
        F.coalesce(F.col("d.mandatory"), F.lit(False)).alias("obrigatorio"),
        "batch_id", "arquivo_origem",
    )
)
demanda = aplicar_regras(demanda, "prata_demanda_time", [
    ("D1", "Data inválida ou fora da semana do lote", fora_da_semana("data")),
    ("D2", "Mínimo ou máximo de pessoas inválido (nulo, negativo ou mínimo > máximo)",
     F.col("min_pessoas").isNull() | F.col("max_pessoas").isNull() | (F.col("min_pessoas") < 0)
     | (F.col("min_pessoas") > F.col("max_pessoas"))),
])
merge_rpa(com_hash(demanda, ["min_pessoas", "max_pessoas", "obrigatorio", "batch_id"]),
          "prata_demanda_time", ["semana", "time_nome", "data"], SEMANAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 `prata_funcionario_semana`: restrições de cada funcionário na semana
# MAGIC A estação é validada e padronizada pelo mesmo cadastro oficial do `02_prata` (linha + nome sem acento).

# COMMAND ----------

pessoas = (
    vigentes.select("batch_id", "semana", "arquivo_origem", "j.entryWindows", F.explode("j.employees").alias("e"))
    .select(
        "semana",
        F.lower(F.trim("e.employeeRef")).alias("employee_ref"),
        F.col("e.teamName").alias("time_nome"),
        F.col("e.lineNumber").alias("linha"),
        F.trim("e.boardingStation").alias("estacao_informada"),
        F.col("e.monthlyOnSiteDays").alias("dias_mes_presencial"),
        F.expr("transform(coalesce(e.unavailableDates, CAST(array() AS ARRAY<STRING>)), x -> try_cast(x AS DATE))").alias("datas_indisponiveis"),
        F.col("e.latestEntryWindow").alias("janela_maxima"),
        F.col("entryWindows").alias("_janelas"),
        "batch_id", "arquivo_origem",
    )
)
cadastro = spark.table(t("prata_estacao")).select(
    "linha", "chave_nome", "estacao_codigo", F.col("estacao_nome").alias("estacao_embarque"))
pessoas = pessoas.withColumn("chave_nome", chave_nome(F.col("estacao_informada"))).join(
    cadastro, ["linha", "chave_nome"], "left")

pessoas = aplicar_regras(pessoas, "prata_funcionario_semana", [
    ("P1", "Funcionário sem referência válida (12 caracteres hexadecimais)",
     ~F.col("employee_ref").rlike("^[0-9a-f]{12}$")),
    ("P2", "Estação não encontrada no cadastro do Metrô (linha + nome)", F.col("estacao_embarque").isNull()),
    ("P3", "Horário máximo de entrada fora das janelas da empresa",
     F.col("janela_maxima").isNotNull() & ~F.array_contains("_janelas", F.col("janela_maxima"))),
    ("P4", "Dias presenciais no mês fora de 0 a 23",
     F.col("dias_mes_presencial").isNull() | ~F.col("dias_mes_presencial").between(0, 23)),
])
tratamentos.append(("prata_funcionario_semana", "Nomes de estação padronizados pelo cadastro oficial",
                    pessoas.filter(F.col("estacao_informada") != F.col("estacao_embarque")).count()))

pessoas = com_hash(
    pessoas.select("semana", "employee_ref", "time_nome", "linha", "estacao_codigo", "estacao_embarque",
                   "dias_mes_presencial", "datas_indisponiveis", "janela_maxima", "batch_id", "arquivo_origem"),
    ["time_nome", "linha", "estacao_codigo", "dias_mes_presencial", "datas_indisponiveis", "janela_maxima", "batch_id"],
)
merge_rpa(pessoas, "prata_funcionario_semana", ["semana", "employee_ref"], SEMANAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 `prata_escala_atual`: em que dias e horários cada pessoa vai hoje (antes do ForaDoPico)
# MAGIC Vem do campo `plannedDays` (formato v2). Dia planejado numa data marcada como indisponível **não é barrado**:
# MAGIC é a escala que a empresa pratica hoje. O registro fica sinalizado e aparece no impacto (seção 4).

# COMMAND ----------

validos = spark.table(t("prata_funcionario_semana")).select(
    "semana", "employee_ref", "datas_indisponiveis", F.lit(True).alias("_cadastrado"))

atual = (
    vigentes.select("batch_id", "semana", "arquivo_origem", "j.entryWindows", F.explode("j.employees").alias("e"))
    .select("batch_id", "semana", "arquivo_origem", F.col("entryWindows").alias("_janelas"),
            F.lower(F.trim("e.employeeRef")).alias("employee_ref"), F.col("e.teamName").alias("time_nome"),
            F.explode_outer("e.plannedDays").alias("p"))
    .filter(F.col("p").isNotNull())   # lote da versão 1 ou pessoa sem escala atual: nada a carregar
    .select("semana", "employee_ref", "time_nome",
            F.expr("try_cast(p.date AS DATE)").alias("data"),
            F.col("p.entryWindow").alias("janela_entrada"),
            "_janelas", "batch_id", "arquivo_origem")
    .join(validos, ["semana", "employee_ref"], "left")
)
atual = aplicar_regras(atual, "prata_escala_atual", [
    ("A1", "Data inválida ou fora da semana do lote", fora_da_semana("data")),
    ("A2", "Horário de entrada fora das janelas da empresa", ~F.array_contains("_janelas", F.col("janela_entrada"))),
    ("A3", "Funcionário barrado na validação do cadastro (seção 2.3)", F.col("_cadastrado").isNull()),
])
atual = atual.withColumn("em_dia_indisponivel", F.array_contains("datas_indisponiveis", F.col("data")))
tratamentos.append(("prata_escala_atual", "Dias planejados em data marcada como indisponível (mantidos e sinalizados)",
                    atual.filter("em_dia_indisponivel").count()))

merge_rpa(
    com_hash(atual.select("semana", "employee_ref", "time_nome", "data", "janela_entrada", "em_dia_indisponivel",
                          "batch_id", "arquivo_origem"),
             ["time_nome", "janela_entrada", "em_dia_indisponivel", "batch_id"]),
    "prata_escala_atual", ["semana", "employee_ref", "data"], SEMANAS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Evidências da Prata: regras, tratamentos, MERGE e quarentena

# COMMAND ----------

regras = spark.createDataFrame(
    qualidade, "tabela string, regra string, descricao string, avaliados long, barrados long"
).withColumn("executado_em", F.lit(EXECUCAO))
regras.write.mode("append").saveAsTable(t("controle_qualidade_prata"))
display(regras.select("tabela", "regra", "descricao", "avaliados", "barrados"))

display(spark.createDataFrame(tratamentos, "tabela string, tratamento string, registros long"))

# COMMAND ----------

spark.createDataFrame(
    merges, "tabela string, linhas_no_lote long, inseridos long, atualizados long, removidos long, total_na_tabela long"
).withColumn("etapa", F.lit(dbutils.widgets.get("etapa"))).withColumn("executado_em", F.lit(EXECUCAO)) \
    .write.mode("append").saveAsTable(t("controle_merge_rpa"))

# a última execução de cada etapa: lote 1, lote 2 e reexecução sem arquivo novo
display(spark.sql(f"""
    SELECT etapa, tabela, linhas_no_lote, inseridos, atualizados, removidos, total_na_tabela
    FROM (
        SELECT *, dense_rank() OVER (PARTITION BY etapa ORDER BY executado_em DESC) AS rodada
        FROM {t('controle_merge_rpa')}
    )
    WHERE rodada = 1
    ORDER BY etapa, tabela
"""))

# COMMAND ----------

(
    reduce(lambda a, b: a.unionByName(b), quarentena)
    .withColumn("barrado_em", F.lit(EXECUCAO))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(t("quarentena_rpa"))
)
display(spark.table(t("quarentena_rpa")).orderBy("tabela", "regra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ouro: a escala sugerida pelo ForaDoPico
# MAGIC ### 3.1 O algoritmo
# MAGIC Para cada semana e time, nesta ordem:
# MAGIC 1. **Dias sem expediente** (feriados da `dim_data`) ficam de fora. Se o RH pediu gente num feriado, vira pendência.
# MAGIC 2. **Meta de cada pessoa na semana** = dias presenciais no mês × dias úteis da semana ÷ dias úteis do mês, arredondada
# MAGIC    e nunca maior que os dias em que ela está disponível.
# MAGIC 3. **Dias obrigatórios** (reunião do time) são preenchidos primeiro, até o máximo. Depois, **todo dia chega ao mínimo**.
# MAGIC 4. Os dias que faltam vão para o **dia mais vazio do time e do metrô da pessoa**, sem passar do máximo do RH.
# MAGIC    **Datas indisponíveis nunca entram.**
# MAGIC 5. **Janela de entrada:** a de menor lotação no embarque, na estação da pessoa naquele dia da semana, respeitando o
# MAGIC    horário máximo dela e com no máximo 40% do time do dia na mesma janela (o mesmo rodízio do `03_ouro`).
# MAGIC
# MAGIC O problema é pequeno (dezenas de pessoas por semana), então roda em Python no driver. O desempate usa um hash da
# MAGIC pessoa e do dia: o resultado é **determinístico** (a mesma entrada sempre gera a mesma escala) e o rodízio é justo.

# COMMAND ----------

def _h(*partes):
    """Desempate determinístico: muda a ordem a cada dia (rodízio justo) e dá o mesmo resultado em toda execução."""
    return hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest()


def sugerir_escala(demanda, funcionarios, calendario, lotacao, janelas_por_semana, limite_janela=0.40):
    """Monta a escala sugerida de cada semana e time.

    demanda:      semana, time_nome, data, min_pessoas, max_pessoas, obrigatorio
    funcionarios: semana, employee_ref, time_nome, estacao_id, dias_mes_presencial, datas_indisponiveis, janela_maxima
    calendario:   data, eh_dia_util, nome_feriado, dias_uteis_mes
    lotacao:      estacao_id, data, janela_entrada, indice_lotacao (no embarque)
    Devolve (presencas, pendencias) em pandas.
    """
    cal = calendario.set_index("data")
    indice = {(r.estacao_id, r.data, r.janela_entrada): r.indice_lotacao for r in lotacao.itertuples()}
    presencas, pendencias = [], []

    def nota(semana, time_nome, data, tipo, detalhe):
        pendencias.append({"semana": semana, "time_nome": time_nome, "data": data, "tipo": tipo, "detalhe": detalhe})

    for (semana, time_nome), dem in demanda.groupby(["semana", "time_nome"]):
        janelas = list(janelas_por_semana[semana])
        dem = dem.sort_values("data")
        equipe = funcionarios[(funcionarios.semana == semana) & (funcionarios.time_nome == time_nome)]
        uteis = []
        for r in dem.itertuples():
            if bool(cal.loc[r.data, "eh_dia_util"]):
                uteis.append(r.data)
            else:
                nota(semana, time_nome, r.data, "Demanda em dia sem expediente",
                     f"{cal.loc[r.data, 'nome_feriado'] or 'Fim de semana'}: demanda de {r.min_pessoas} a {r.max_pessoas} pessoas ignorada")
        regra = {r.data: r for r in dem.itertuples() if r.data in uteis}
        presentes = {d: [] for d in uteis}
        pessoa = {e.employee_ref: e for e in equipe.itertuples()}

        def permitidas(e):
            limite = e.janela_maxima if isinstance(e.janela_maxima, str) else None
            return [j for j in janelas if limite is None or j <= limite]

        def custo(e, d):  # melhor lotação que a pessoa consegue naquele dia
            return min(indice.get((e.estacao_id, d, j), 1.0) for j in permitidas(e))

        disponiveis, restantes = {}, {}
        for e in equipe.itertuples():
            fora = set(e.datas_indisponiveis) if e.datas_indisponiveis is not None else set()
            disponiveis[e.employee_ref] = [d for d in uteis if d not in fora]
            dias_mes = cal.loc[uteis[0], "dias_uteis_mes"] if uteis else 1
            alvo = math.floor(e.dias_mes_presencial * len(uteis) / dias_mes + 0.5)
            restantes[e.employee_ref] = max(1, min(len(disponiveis[e.employee_ref]), alvo))

        def escolher(d, so_com_saldo):
            livres = [r for r in pessoa if d in disponiveis[r] and r not in presentes[d]
                      and (restantes[r] > 0 or not so_com_saldo)]
            livres.sort(key=lambda r: (-restantes[r], len(disponiveis[r]), custo(pessoa[r], d), _h(r, d)))
            return livres[0] if livres else None

        # 1) dias obrigatórios (reunião do time): preenche até o máximo
        for d in [d for d in uteis if regra[d].obrigatorio]:
            while len(presentes[d]) < regra[d].max_pessoas and (r := escolher(d, True)):
                presentes[d].append(r); restantes[r] -= 1
        # 2) todo dia chega ao mínimo (usa dia extra só se faltar gente)
        for d in sorted(uteis, key=lambda d: -(regra[d].min_pessoas - len(presentes[d]))):
            while len(presentes[d]) < regra[d].min_pessoas:
                r = escolher(d, True) or escolher(d, False)
                if r is None:
                    nota(semana, time_nome, d, "Mínimo não atingido",
                         f"{len(presentes[d])} de {regra[d].min_pessoas} pessoas disponíveis")
                    break
                if restantes[r] <= 0:
                    nota(semana, time_nome, d, "Dia extra para cobrir o mínimo", r)
                presentes[d].append(r); restantes[r] -= 1
        # 3) os dias que faltam: o dia mais vazio no time e no metrô da pessoa
        progresso = True
        while progresso:
            progresso = False
            for r in sorted(pessoa, key=lambda r: (-restantes[r], _h(r, semana))):
                if restantes[r] <= 0:
                    continue
                opcoes = [d for d in disponiveis[r] if r not in presentes[d] and len(presentes[d]) < regra[d].max_pessoas]
                if not opcoes:
                    continue
                d = min(opcoes, key=lambda d: (len(presentes[d]) / regra[d].max_pessoas, custo(pessoa[r], d), _h(r, d)))
                presentes[d].append(r); restantes[r] -= 1; progresso = True
        for r, falta in restantes.items():
            if falta > 0:
                nota(semana, time_nome, None, "Meta semanal não cumprida", f"{r}: faltaram {falta} dia(s) por limite do time ou indisponibilidade")

        # 4) janela de entrada: a de menor lotação no embarque, com no máximo 40% do time do dia por janela
        for d in uteis:
            vagas = math.ceil(len(presentes[d]) * limite_janela)
            ocupacao = {j: 0 for j in janelas}
            ordem = sorted(presentes[d], key=lambda r: (len(permitidas(pessoa[r])), _h(r, d)))
            for r in ordem:
                e = pessoa[r]
                candidatas = sorted(permitidas(e), key=lambda j: (indice.get((e.estacao_id, d, j), 1.0), j))
                livre = [j for j in candidatas if ocupacao[j] < vagas]
                j = livre[0] if livre else candidatas[0]
                if not livre:
                    nota(semana, time_nome, d, "Janela acima do limite de 40%", f"{r} em {j}")
                ocupacao[j] += 1
                presencas.append({"semana": semana, "employee_ref": r, "time_nome": time_nome,
                                  "data": d, "janela_entrada": j})
    return (pd.DataFrame(presencas, columns=["semana", "employee_ref", "time_nome", "data", "janela_entrada"]),
            pd.DataFrame(pendencias, columns=["semana", "time_nome", "data", "tipo", "detalhe"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Entradas do algoritmo: demanda, pessoas, calendário e lotação no embarque
# MAGIC Para cada estação, dia e janela permitida, a lotação prevista vem da `ouro_lotacao_prevista` na faixa do embarque
# MAGIC (1 hora antes da entrada), pelo dia da semana. Feriados e emendas vêm da `dim_data`.

# COMMAND ----------

dim_data = spark.table(t("dim_data"))
uteis_mes = dim_data.groupBy("ano", "mes").agg(F.sum(F.col("eh_dia_util").cast("int")).alias("dias_uteis_mes"))
prevista = spark.table(t("ouro_lotacao_prevista")).select(
    "estacao_id", "dia_semana_iso", "faixa_id", "indice_lotacao", "categoria")


def no_embarque(df):
    """Hora do embarque e lotação prevista nela, para a coluna janela_entrada (precisa de estacao_id e dia_semana_iso)."""
    embarque = minutos("janela_entrada") - TEMPO_VIAGEM_MIN
    return (
        df.withColumn("embarque", F.format_string("%02d:%02d", F.floor(embarque / 60), embarque % 60))
        .withColumn("faixa_id", F.floor(embarque / 15).cast("int"))
        .join(prevista, ["estacao_id", "dia_semana_iso", "faixa_id"], "left")
    )


dem_spark = spark.table(t("prata_demanda_time")).filter(F.col("semana").isin(SEMANAS))
func_spark = (
    spark.table(t("prata_funcionario_semana")).filter(F.col("semana").isin(SEMANAS))
    .withColumn("estacao_id", F.concat_ws("-", F.col("linha").cast("string"), "estacao_codigo"))
)
janelas_lote = spark.table(t("prata_lote_escala")).filter("vigente").select("semana", "janelas_permitidas")
JANELAS = {r.semana: list(r.janelas_permitidas) for r in janelas_lote.collect()}

cal_spark = (
    dem_spark.select("data").distinct().join(dim_data, "data").join(uteis_mes, ["ano", "mes"])
    .select(F.col("data").cast("string").alias("data"), "eh_dia_util", "nome_feriado", "dias_uteis_mes")
)
assert dem_spark.select("data").distinct().count() == cal_spark.count(), \
    "Há datas da demanda fora da dim_data: rode o 03_ouro com o calendário do ano certo"

opcoes = no_embarque(
    func_spark.select("semana", "estacao_id").distinct()
    .join(dem_spark.select("semana", "data").distinct(), "semana")
    .join(dim_data.select("data", "dia_semana_iso"), "data")
    .join(janelas_lote.select("semana", F.explode("janelas_permitidas").alias("janela_entrada")), "semana")
).select("estacao_id", F.col("data").cast("string").alias("data"), "janela_entrada",
         F.coalesce("indice_lotacao", F.lit(1.0)).alias("indice_lotacao"))

dem_pd = dem_spark.select("semana", "time_nome", F.col("data").cast("string").alias("data"),
                          "min_pessoas", "max_pessoas", "obrigatorio").toPandas()
func_pd = func_spark.select(
    "semana", "employee_ref", "time_nome", "estacao_id", "dias_mes_presencial",
    F.expr("transform(datas_indisponiveis, x -> CAST(x AS STRING))").alias("datas_indisponiveis"), "janela_maxima",
).toPandas()

sug_pd, pend_pd = sugerir_escala(dem_pd, func_pd, cal_spark.toPandas(), opcoes.toPandas(), JANELAS, LIMITE_POR_JANELA)
print(f"Semanas: {SEMANAS} | pessoas: {len(func_pd)} | dias presenciais sugeridos: {len(sug_pd)} | pendências: {len(pend_pd)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 `fato_escala_rpa`: cada dia presencial nos dois cenários
# MAGIC - **Atual (RH):** a escala que a empresa pratica hoje (`plannedDays` do lote).
# MAGIC - **Sugerida (ForaDoPico):** a escala montada na seção 3.1.
# MAGIC
# MAGIC Para cada viagem: horário de embarque, lotação prevista, se cai no pico e se fere alguma restrição da pessoa.
# MAGIC O funcionário aparece só pelo `employee_ref` pseudonimizado pelo robô (LGPD).

# COMMAND ----------

sugerida = (
    spark.createDataFrame(sug_pd, "semana string, employee_ref string, time_nome string, data string, janela_entrada string")
    .withColumn("data", F.to_date("data"))
    .withColumn("cenario", F.lit(SUGERIDA))
)
atual = (
    spark.table(t("prata_escala_atual")).filter(F.col("semana").isin(SEMANAS))
    .select("semana", "employee_ref", "time_nome", "data", "janela_entrada")
    .withColumn("cenario", F.lit(ATUAL))
)
viagens = no_embarque(
    atual.unionByName(sugerida)
    .join(func_spark.select("semana", "employee_ref", "estacao_id", "estacao_embarque",
                            "datas_indisponiveis", "janela_maxima"), ["semana", "employee_ref"])
    .join(dim_data.select("data", "dia_semana_iso", "dia_semana_nome", "eh_dia_util"), "data")
).join(spark.table(t("dim_faixa_horaria")).select("faixa_id", "eh_pico"), "faixa_id", "left")

fato = viagens.select(
    "semana", "data", "dia_semana_nome", "cenario", "employee_ref", "time_nome", "estacao_id", "estacao_embarque",
    "janela_entrada", "embarque", "indice_lotacao", "categoria",
    F.coalesce("eh_pico", F.lit(False)).alias("eh_pico"),
    F.array_contains("datas_indisponiveis", F.col("data")).alias("em_dia_indisponivel"),
    (F.col("janela_maxima").isNotNull() & (F.col("janela_entrada") > F.col("janela_maxima"))).alias("apos_horario_maximo"),
    (~F.col("eh_dia_util")).alias("em_dia_sem_expediente"),
)
salvar(fato, "fato_escala_rpa")
display(spark.table(t("fato_escala_rpa")).orderBy("employee_ref", "data", "cenario").limit(12))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Aderência à demanda do RH e pendências
# MAGIC Para cada time e dia: quantas pessoas cada cenário coloca no escritório, comparado com o mínimo e o máximo pedidos.

# COMMAND ----------

presentes = spark.table(t("fato_escala_rpa")).groupBy("semana", "time_nome", "data", "cenario").agg(
    F.count("*").alias("presentes"))
cenarios = spark.createDataFrame([(ATUAL,), (SUGERIDA,)], "cenario string")

aderencia = (
    dem_spark.crossJoin(cenarios)
    .join(presentes, ["semana", "time_nome", "data", "cenario"], "left")
    .join(dim_data.select("data", "dia_semana_nome", "eh_dia_util", "nome_feriado"), "data")
    .withColumn("presentes", F.coalesce("presentes", F.lit(0)))
    .withColumn(
        "situacao",
        F.when(~F.col("eh_dia_util") & (F.col("presentes") > 0), "Gente escalada em dia sem expediente")
        .when(~F.col("eh_dia_util"), F.concat(F.lit("Sem expediente: "), F.coalesce("nome_feriado", F.lit("fim de semana"))))
        .when(F.col("presentes") < F.col("min_pessoas"), "Abaixo do mínimo")
        .when(F.col("presentes") > F.col("max_pessoas"), "Acima do máximo")
        .otherwise("Dentro da demanda"),
    )
    .select("semana", "data", "dia_semana_nome", "time_nome", "cenario", "min_pessoas", "max_pessoas",
            "obrigatorio", "eh_dia_util", "presentes", "situacao")
)
salvar(aderencia, "ouro_aderencia_demanda_rpa")

display(spark.sql(f"""
    SELECT semana, data, dia_semana_nome AS dia, time_nome AS time,
           concat(min_pessoas, ' a ', max_pessoas, IF(obrigatorio, ' (obrigatório)', '')) AS demanda_do_rh,
           max(IF(cenario = '{ATUAL}', presentes, NULL)) AS presentes_atual,
           max(IF(cenario = '{ATUAL}', situacao, NULL)) AS situacao_atual,
           max(IF(cenario = '{SUGERIDA}', presentes, NULL)) AS presentes_sugerida,
           max(IF(cenario = '{SUGERIDA}', situacao, NULL)) AS situacao_sugerida
    FROM {t('ouro_aderencia_demanda_rpa')}
    GROUP BY semana, data, dia_semana_nome, time_nome, min_pessoas, max_pessoas, obrigatorio
    ORDER BY data, time
"""))

salvar(spark.createDataFrame(pend_pd, "semana string, time_nome string, data string, tipo string, detalhe string"),
       "ouro_pendencias_escala_rpa")
display(spark.table(t("ouro_pendencias_escala_rpa")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consulta 6 (impacto com dados reais do RH): a escala sugerida melhora a da empresa?

# COMMAND ----------

display(spark.sql(f"""
    WITH v AS (
        SELECT cenario,
               CAST(count(*) AS DOUBLE) AS viagens,
               round(100 * avg(CAST(eh_pico AS INT)), 1) AS pct_pico,
               CAST(sum(CAST(categoria = 'Crítico' AS INT)) AS DOUBLE) AS critico,
               round(avg(indice_lotacao), 2) AS indice,
               CAST(sum(CAST(em_dia_indisponivel AS INT)) AS DOUBLE) AS indisponivel,
               CAST(sum(CAST(apos_horario_maximo AS INT)) AS DOUBLE) AS apos_maximo
        FROM {t('fato_escala_rpa')}
        GROUP BY cenario
    ),
    a AS (
        SELECT cenario, CAST(sum(CAST(eh_dia_util AND situacao <> 'Dentro da demanda' AS INT)) AS DOUBLE) AS fora_demanda
        FROM {t('ouro_aderencia_demanda_rpa')}
        GROUP BY cenario
    ),
    r AS (SELECT * FROM v JOIN a USING (cenario))
    SELECT indicador,
           max(IF(cenario = '{ATUAL}', valor, NULL)) AS escala_atual_rh,
           max(IF(cenario = '{SUGERIDA}', valor, NULL)) AS escala_sugerida
    FROM r
    LATERAL VIEW stack(7,
        '1. Viagens presenciais', viagens,
        '2. % das viagens no horário de pico', pct_pico,
        '3. Viagens em faixa crítica', critico,
        '4. Índice médio de lotação no embarque', indice,
        '5. Dias de time fora do mínimo ou máximo do RH', fora_demanda,
        '6. Viagens em dia marcado como indisponível', indisponivel,
        '7. Entradas depois do horário máximo da pessoa', apos_maximo
    ) s AS indicador, valor
    GROUP BY indicador
    ORDER BY indicador
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Por semana**

# COMMAND ----------

display(spark.sql(f"""
    SELECT semana, cenario, count(*) AS viagens,
           round(100 * avg(CAST(eh_pico AS INT)), 1) AS pct_no_pico,
           sum(CAST(categoria = 'Crítico' AS INT)) AS em_faixa_critica,
           round(avg(indice_lotacao), 2) AS indice_medio
    FROM {t('fato_escala_rpa')}
    GROUP BY semana, cenario
    ORDER BY semana, cenario
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** Nas duas semanas da planilha do RH (16 a 20/11 e 23 a 27/11), a escala que a empresa pratica hoje concentra todo mundo de terça a quinta, entrando às 07:00 ou às 08:00: 100% das 117 viagens caem no horário de pico, 75 em faixa crítica, e os 18 dias de time ficam fora do mínimo ou do máximo pedidos pelo próprio RH (ninguém na segunda e na sexta, gente demais no meio da semana). Ela ainda escala 4 viagens em dias que os funcionários marcaram como indisponíveis. A escala sugerida pelo ForaDoPico cumpre a demanda nos 18 dias, respeita todas as indisponibilidades e horários máximos, reduz as viagens no pico para 16,1%, zera as viagens em faixa crítica e baixa o índice médio de lotação no embarque de 0,86 para 0,34 (-60%). O total de viagens cai de 117 para 93 porque a escala atual põe mais gente no escritório do que o máximo do RH e do que a meta mensal de cada pessoa. A integração também encontrou um erro na planilha: ela pedia gente em 20/11, feriado nacional (Dia da Consciência Negra), e isso virou pendência para o RH.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Uso pelo MVP: `vw_app_escala_rpa`
# MAGIC A tela **Minha semana** com a escala real do RH: dia, horário de entrada sugerido, hora de embarque e lotação prevista,
# MAGIC ao lado do horário que a pessoa pratica hoje. Sem nome, e-mail ou matrícula: só o `employee_ref` do robô.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('vw_app_escala_rpa')} AS
    SELECT s.employee_ref, s.semana, s.data, s.dia_semana_nome, s.time_nome, s.estacao_embarque,
           s.janela_entrada AS entrar_no_escritorio, s.embarque AS embarcar_ate, s.categoria AS lotacao_prevista,
           a.janela_entrada AS horario_atual
    FROM {t('fato_escala_rpa')} s
    LEFT JOIN {t('fato_escala_rpa')} a
           ON a.cenario = '{ATUAL}' AND a.employee_ref = s.employee_ref AND a.data = s.data
    WHERE s.cenario = '{SUGERIDA}'
""")

exemplo = spark.table(t("vw_app_escala_rpa")).orderBy("employee_ref").first()
if exemplo:
    display(spark.table(t("vw_app_escala_rpa")).filter(F.col("employee_ref") == exemplo.employee_ref).orderBy("data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências para o relatório
# MAGIC | Etapa | O que mostrar |
# MAGIC | --- | --- |
# MAGIC | `1_lote_w47` | Seção 1 (1 lote na Bronze) e 2.5 (MERGE: tudo inserido) |
# MAGIC | `2_lote_w48` | Seção 1 (2 lotes), 1.1 (cada lote uma única carga) e 2.5 (W48 inserida, W47 sem alteração) |
# MAGIC | `3_reexecucao` | Seção 1 (contagem igual) e 2.5 (0 inseridos, 0 atualizados, 0 removidos) |
# MAGIC | Final | 2.5 (regras e tratamentos), 3.4 (aderência e pendências), 4 (consulta 6) e 5 (view do app) |
