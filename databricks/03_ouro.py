# Databricks notebook source
# MAGIC %md
# MAGIC # ForaDoPico · 03 · Camada Ouro
# MAGIC
# MAGIC Organiza os dados da Prata para responder às perguntas do produto (requisitos 07 e 08).
# MAGIC
# MAGIC | Passo | Seção | O que cria |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Dimensões | `dim_data`, `dim_estacao`, `dim_faixa_horaria` |
# MAGIC | 2 | Fato de lotação | `fato_lotacao` e a previsão `ouro_lotacao_prevista` |
# MAGIC | 3 | Escala e LGPD | `dim_funcionario` (com hash e máscara) e `fato_viagem_escala` |
# MAGIC | 4 | Integração com o RPA | notebook `04_integracao_rpa` (lotes do robô) |
# MAGIC | 5 | As 5 consultas SQL | respostas às perguntas do MVP |
# MAGIC | 6 | Uso pelo MVP | views que o app lê, sem dado pessoal |
# MAGIC
# MAGIC **Hipótese documentada:** o Metrô publica entradas **diárias** por estação. A lotação por faixa de 15 minutos é estimada
# MAGIC distribuindo o total do dia por um **perfil horário** (seção 1), que pode ser trocado por dados horários reais,
# MAGIC por exemplo a contagem de pessoas da visão computacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuração

# COMMAND ----------

import math
import secrets

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "workspace"
SCHEMA = "foradopico"

TEMPO_VIAGEM_MIN = 60                         # hipótese: embarque 1 hora antes da entrada no escritório
JANELAS_ENTRADA = ["07:00", "08:00", "09:30", "10:30"]  # janelas permitidas pela empresa
LIMITE_POR_JANELA = 0.40                      # no máximo 40% do time na mesma janela (rodízio)
LIMITE_PICO = 0.60                            # faixa de pico: pelo menos 60% do movimento da faixa mais cheia


def t(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def salvar(df, nome: str) -> None:
    """A Ouro é recalculada inteira a partir da Prata a cada execução."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(nome))
    print(f"{nome}: {spark.table(t(nome)).count()} linhas")


def categoria(indice):
    return (
        F.when(indice < 0.35, "Vazio")
        .when(indice < 0.60, "Moderado")
        .when(indice < 0.85, "Cheio")
        .otherwise("Crítico")
    )


def minutos(coluna_hhmm: str):
    return F.expr(f"try_cast(substr({coluna_hhmm}, 1, 2) AS INT) * 60 + try_cast(substr({coluna_hhmm}, 4, 2) AS INT)")


spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
print("Configuração pronta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensões
# MAGIC ### 1.1 `dim_faixa_horaria`: 96 faixas de 15 minutos e o perfil horário
# MAGIC Perfil de dia útil com pico às 7h e às 18h e um pico menor no almoço; fim de semana mais espalhado.
# MAGIC O Metrô abre às 4h40, então as faixas antes das 4h45 têm peso zero. Os pesos de cada dia somam 1.

# COMMAND ----------

def _curva(m, centro, largura):
    return math.exp(-((m - centro) / largura) ** 2 / 2)


def _dia_util(m):
    return 0.0 if m < 285 else 0.12 + _curva(m, 430, 60) + 0.75 * _curva(m, 1080, 60) + 0.22 * _curva(m, 750, 90)


def _fim_de_semana(m):
    return 0.0 if m < 285 else 0.25 + 0.55 * _curva(m, 690, 150) + 0.45 * _curva(m, 1020, 120)


def _periodo(m):
    return "Madrugada" if m < 360 else "Manhã" if m < 720 else "Tarde" if m < 1080 else "Noite"


meios = [i * 15 + 7.5 for i in range(96)]
peso_util = [_dia_util(m) for m in meios]
peso_util = [p / sum(peso_util) for p in peso_util]
peso_fds = [_fim_de_semana(m) for m in meios]
peso_fds = [p / sum(peso_fds) for p in peso_fds]
maior = max(peso_util)

faixas = [
    (
        i,
        f"{(i * 15) // 60:02d}:{(i * 15) % 60:02d}",
        (i * 15) // 60,
        i * 15,
        _periodo(i * 15),
        peso_util[i] >= LIMITE_PICO * maior,
        round(peso_util[i], 6),
        round(peso_fds[i], 6),
    )
    for i in range(96)
]
dim_faixa = spark.createDataFrame(
    faixas,
    "faixa_id int, inicio string, hora int, minuto_do_dia int, periodo string, "
    "eh_pico boolean, peso_dia_util double, peso_fim_de_semana double",
)
salvar(dim_faixa, "dim_faixa_horaria")

# Ordem das janelas da menos para a mais lotada no embarque, usada no rodízio da escala sugerida
def _indice_janela(janela):
    h, m = map(int, janela.split(":"))
    return peso_util[(h * 60 + m - TEMPO_VIAGEM_MIN) // 15] / maior


JANELAS_POR_LOTACAO = sorted(JANELAS_ENTRADA, key=_indice_janela)
print("Janelas da mais vazia para a mais cheia:", JANELAS_POR_LOTACAO)

display(spark.table(t("dim_faixa_horaria")).filter("eh_pico").orderBy("faixa_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 `dim_data`: calendário de 2026 com feriados e emendas
# MAGIC **Emenda** é o dia útil espremido entre um feriado e o fim de semana (ex.: sexta depois de feriado na quinta).

# COMMAND ----------

ordem = Window.orderBy("data")
dias = spark.sql("SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS data")

dim_data = (
    dias.join(spark.table(t("prata_feriados")).select("data", "nome_feriado"), "data", "left")
    .withColumn("dia_semana_iso", ((F.dayofweek("data") + 5) % 7) + 1)  # 1 = segunda ... 7 = domingo
    .withColumn("eh_feriado", F.col("nome_feriado").isNotNull())
    .withColumn("_folga", F.col("eh_feriado") | (F.col("dia_semana_iso") >= 6))
    .withColumn("_folga_antes", F.coalesce(F.lag("_folga").over(ordem), F.lit(False)))
    .withColumn("_folga_depois", F.coalesce(F.lead("_folga").over(ordem), F.lit(False)))
    .withColumn("_feriado_antes", F.coalesce(F.lag("eh_feriado").over(ordem), F.lit(False)))
    .withColumn("_feriado_depois", F.coalesce(F.lead("eh_feriado").over(ordem), F.lit(False)))
    .withColumn(
        "tipo_dia",
        F.when(F.col("eh_feriado"), "Feriado")
        .when(F.col("dia_semana_iso") == 6, "Sábado")
        .when(F.col("dia_semana_iso") == 7, "Domingo")
        .when(
            (F.col("_feriado_antes") & F.col("_folga_depois")) | (F.col("_feriado_depois") & F.col("_folga_antes")),
            "Emenda",
        )
        .otherwise("Dia útil"),
    )
    .select(
        "data",
        F.year("data").alias("ano"),
        F.month("data").alias("mes"),
        "dia_semana_iso",
        F.element_at(
            F.array(*[F.lit(d) for d in ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]]),
            F.col("dia_semana_iso"),
        ).alias("dia_semana_nome"),
        "tipo_dia",
        F.col("tipo_dia").isin("Dia útil", "Emenda").alias("eh_dia_util"),
        "eh_feriado",
        "nome_feriado",
    )
)
salvar(dim_data, "dim_data")

display(spark.sql(f"""
    SELECT tipo_dia, count(*) AS dias
    FROM {t('dim_data')}
    GROUP BY tipo_dia
    ORDER BY dias DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 `dim_estacao`: estações das Linhas 1-Azul e 3-Vermelha

# COMMAND ----------

dim_estacao = spark.table(t("prata_estacao")).select(
    F.concat_ws("-", F.col("linha").cast("string"), "estacao_codigo").alias("estacao_id"),
    "linha",
    F.when(F.col("linha") == 1, "1-Azul")
    .when(F.col("linha") == 3, "3-Vermelha")
    .otherwise(F.concat(F.lit("Linha "), F.col("linha").cast("string")))
    .alias("linha_nome"),
    "estacao_codigo",
    "estacao_nome",
)
salvar(dim_estacao, "dim_estacao")
display(spark.table(t("dim_estacao")).orderBy("linha", "estacao_nome"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fato de lotação
# MAGIC ### 2.1 `fato_lotacao`: estação × dia × faixa de 15 minutos
# MAGIC `entradas_estimadas` = entradas reais do dia × peso da faixa (perfil de dia útil ou de fim de semana e feriado).
# MAGIC O **índice de lotação** compara cada faixa com a referência da própria estação (percentil 99 das suas faixas),
# MAGIC porque cada estação tem um tamanho diferente. Categorias: Vazio (< 0,35), Moderado (< 0,60), Cheio (< 0,85) e Crítico.

# COMMAND ----------

lotacao = (
    spark.table(t("prata_demanda_metro"))
    .join(spark.table(t("dim_data")).select("data", "eh_dia_util"), "data")
    .crossJoin(
        spark.table(t("dim_faixa_horaria"))
        .filter("peso_dia_util > 0")
        .select("faixa_id", "peso_dia_util", "peso_fim_de_semana")
    )
    .select(
        "data",
        F.concat_ws("-", F.col("linha").cast("string"), "estacao_codigo").alias("estacao_id"),
        "faixa_id",
        F.round(
            F.col("entradas")
            * F.when(F.col("eh_dia_util"), F.col("peso_dia_util")).otherwise(F.col("peso_fim_de_semana")),
            1,
        ).alias("entradas_estimadas"),
    )
)
referencia = lotacao.groupBy("estacao_id").agg(
    F.expr("percentile_approx(entradas_estimadas, 0.99)").alias("referencia")
)
fato_lotacao = (
    lotacao.join(referencia, "estacao_id")
    .withColumn("indice_lotacao", F.round(F.col("entradas_estimadas") / F.col("referencia"), 3))
    .withColumn("categoria", categoria(F.col("indice_lotacao")))
    .drop("referencia")
)
salvar(fato_lotacao, "fato_lotacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 `ouro_lotacao_prevista`: a lotação típica por estação, dia da semana e faixa
# MAGIC É a **previsão** que o app mostra: a média dos dias equivalentes da base (dias úteis comuns de segunda a sexta,
# MAGIC sábados e domingos), sem feriados nem emendas para não distorcer.

# COMMAND ----------

prevista = (
    spark.table(t("fato_lotacao"))
    .join(spark.table(t("dim_data")).select("data", "dia_semana_iso", "tipo_dia"), "data")
    .filter(F.col("tipo_dia").isin("Dia útil", "Sábado", "Domingo"))
    .groupBy("estacao_id", "dia_semana_iso", "faixa_id")
    .agg(
        F.round(F.avg("entradas_estimadas"), 1).alias("entradas_previstas"),
        F.round(F.avg("indice_lotacao"), 3).alias("indice_lotacao"),
        F.countDistinct("data").alias("dias_na_base"),
    )
    .withColumn("categoria", categoria(F.col("indice_lotacao")))
)
salvar(prevista, "ouro_lotacao_prevista")

display(spark.sql(f"""
    SELECT 'fato_lotacao' AS tabela, count(*) AS linhas, count(DISTINCT estacao_id) AS estacoes,
           min(data) AS de, max(data) AS ate
    FROM {t('fato_lotacao')}
    UNION ALL
    SELECT 'ouro_lotacao_prevista', count(*), count(DISTINCT estacao_id), NULL, NULL
    FROM {t('ouro_lotacao_prevista')}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escala presencial e LGPD
# MAGIC ### 3.1 `dim_funcionario`: dados pessoais protegidos (requisito 08)
# MAGIC - `funcionario_chave`: SHA-256 do id com um **salt secreto**, guardado no volume `controle` (fora do código e do GitHub).
# MAGIC - `nome_mascarado`: só a primeira letra de cada nome (ex.: `A** S****`).
# MAGIC - `email_hash`: SHA-256 do e-mail com o mesmo salt. Nome e e-mail originais **não entram na Ouro**.

# COMMAND ----------

CAMINHO_SALT = f"/Volumes/{CATALOGO}/{SCHEMA}/controle/lgpd_salt.txt"
try:
    SALT = dbutils.fs.head(CAMINHO_SALT).strip()
except Exception:
    SALT = secrets.token_hex(32)  # criado uma única vez; nunca aparece no código nem no repositório
    dbutils.fs.put(CAMINHO_SALT, SALT, overwrite=False)


def chave_funcionario(coluna):
    return F.substring(F.sha2(F.concat(F.lit(SALT), coluna), 256), 1, 16)


mais_recente = Window.partitionBy("funcionario_id").orderBy(F.col("data").desc())
dim_funcionario = (
    spark.table(t("prata_escala"))
    .withColumn("_ordem", F.row_number().over(mais_recente))
    .filter("_ordem = 1")
    .select(
        chave_funcionario(F.col("funcionario_id")).alias("funcionario_chave"),
        F.regexp_replace("nome", r"(?<=\p{L})\p{L}", "*").alias("nome_mascarado"),
        F.sha2(F.concat(F.lit(SALT), F.col("email")), 256).alias("email_hash"),
        "empresa_id",
        "empresa_nome",
        "time_nome",
        F.concat_ws("-", F.col("linha").cast("string"), "estacao_codigo").alias("estacao_id"),
    )
)
salvar(dim_funcionario, "dim_funcionario")
display(spark.table(t("dim_funcionario")).orderBy("time_nome", "nome_mascarado").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 `fato_viagem_escala`: escala original × escala sugerida
# MAGIC - **Original:** a escala do RH, com todo mundo entrando às 08:00.
# MAGIC - **Sugerida:** em cada dia, as pessoas presenciais de cada time são distribuídas nas janelas permitidas, da menos
# MAGIC   para a mais lotada, com no máximo 40% do time por janela. A ordem muda a cada dia (rodízio justo).
# MAGIC - Para cada viagem: lotação prevista no embarque (1 hora antes da entrada) e se chovia nessa hora.

# COMMAND ----------

grupo = Window.partitionBy("data", "empresa_id", "time_nome")
rodizio = grupo.orderBy(F.sha2(F.concat_ws("|", "funcionario_id", F.col("data").cast("string")), 256))

viagens = (
    spark.table(t("prata_escala"))
    .filter("presencial")
    .join(spark.table(t("dim_data")).select("data", "dia_semana_iso"), "data")
    .select(
        "data",
        "dia_semana_iso",
        "funcionario_id",
        "empresa_id",
        "time_nome",
        F.concat_ws("-", F.col("linha").cast("string"), "estacao_codigo").alias("estacao_id"),
        F.col("janela_entrada").alias("janela_original"),
    )
    .withColumn("_pessoas", F.count("*").over(grupo))
    .withColumn("_posicao", F.row_number().over(rodizio))
    .withColumn("_vagas", F.ceil(F.col("_pessoas") * LIMITE_POR_JANELA))
    .withColumn(
        "_janela",
        F.least(F.floor((F.col("_posicao") - 1) / F.col("_vagas")), F.lit(len(JANELAS_POR_LOTACAO) - 1)),
    )
    .withColumn(
        "janela_sugerida",
        F.element_at(F.array(*[F.lit(j) for j in JANELAS_POR_LOTACAO]), (F.col("_janela") + 1).cast("int")),
    )
)

prevista = spark.table(t("ouro_lotacao_prevista")).select("estacao_id", "dia_semana_iso", "faixa_id", "indice_lotacao", "categoria")
faixa = spark.table(t("dim_faixa_horaria")).select("faixa_id", "eh_pico")
chuva = spark.table(t("prata_clima")).select("data", "hora", "precipitacao_mm")


def com_lotacao(df, cenario: str):
    """Embarque, lotação prevista, pico e chuva para a janela do cenário ('original' ou 'sugerida')."""
    embarque = minutos(f"janela_{cenario}") - TEMPO_VIAGEM_MIN
    df = (
        df.withColumn(f"embarque_{cenario}", F.format_string("%02d:%02d", F.floor(embarque / 60), embarque % 60))
        .withColumn("faixa_id", F.floor(embarque / 15).cast("int"))
        .withColumn("hora", F.floor(embarque / 60).cast("int"))
    )
    return (
        df.join(prevista, ["estacao_id", "dia_semana_iso", "faixa_id"], "left")
        .join(faixa, "faixa_id", "left")
        .join(chuva, ["data", "hora"], "left")
        .withColumnRenamed("indice_lotacao", f"indice_{cenario}")
        .withColumnRenamed("categoria", f"categoria_{cenario}")
        .withColumnRenamed("eh_pico", f"pico_{cenario}")
        .withColumnRenamed("precipitacao_mm", f"chuva_{cenario}_mm")
        .drop("faixa_id", "hora")
    )


fato_viagem = com_lotacao(com_lotacao(viagens, "original"), "sugerida").select(
    "data",
    chave_funcionario(F.col("funcionario_id")).alias("funcionario_chave"),
    "empresa_id",
    "time_nome",
    "estacao_id",
    "janela_original", "embarque_original", "indice_original", "categoria_original", "pico_original", "chuva_original_mm",
    "janela_sugerida", "embarque_sugerida", "indice_sugerida", "categoria_sugerida", "pico_sugerida", "chuva_sugerida_mm",
)
salvar(fato_viagem, "fato_viagem_escala")
display(spark.table(t("fato_viagem_escala")).orderBy("data", "time_nome").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integração com o RPA
# MAGIC Os lotes de escala gerados pelo robô RPA (planilha do RH validada e pseudonimizada) entram pelo notebook
# MAGIC **`04_integracao_rpa`**, que usa as tabelas desta Ouro para recomendar a janela de entrada de cada funcionário da semana.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. As 5 consultas do MVP
# MAGIC ### Consulta 1: quais estações são as mais movimentadas em dia útil, e em que horário elas lotam?

# COMMAND ----------

display(spark.sql(f"""
    WITH por_dia AS (
        SELECT l.estacao_id, l.data,
               sum(l.entradas_estimadas) AS entradas_dia,
               max(l.entradas_estimadas) AS pico_15min,
               max_by(f.inicio, l.entradas_estimadas) AS faixa_mais_cheia
        FROM {t('fato_lotacao')} l
        JOIN {t('dim_data')} d ON d.data = l.data
        JOIN {t('dim_faixa_horaria')} f ON f.faixa_id = l.faixa_id
        WHERE d.tipo_dia = 'Dia útil'
        GROUP BY l.estacao_id, l.data
    )
    SELECT e.linha_nome, e.estacao_nome,
           round(avg(p.entradas_dia)) AS media_entradas_por_dia,
           round(avg(p.pico_15min)) AS media_entradas_no_pico_15min,
           min(p.faixa_mais_cheia) AS faixa_mais_cheia
    FROM por_dia p
    JOIN {t('dim_estacao')} e ON e.estacao_id = p.estacao_id
    GROUP BY e.linha_nome, e.estacao_nome
    ORDER BY media_entradas_por_dia DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** As estações com mais entradas em dia útil são Palmeiras-Barra Funda (143 mil/dia), República e Luz (121 mil/dia cada), todas pontos de integração com trem, ônibus ou outras linhas. Em todas, a faixa mais cheia é a das 07:00, com até 5,3 mil entradas em 15 minutos na Barra Funda. Como o perfil horário é o mesmo para todas as estações, o horário do pico é uma hipótese do modelo; o volume diário vem dos dados reais do Metrô.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 2: para quem embarca na estação escolhida, qual a melhor janela de entrada no escritório?
# MAGIC Escolha a estação no campo **estacao** no topo do notebook.

# COMMAND ----------

dbutils.widgets.text("estacao", "Corinthians-Itaquera", "Estação (consulta 2)")
estacao_escolhida = dbutils.widgets.get("estacao").replace("'", "")

janelas_sql = ", ".join(f"('{j}')" for j in JANELAS_ENTRADA)
display(spark.sql(f"""
    WITH janelas AS (
        SELECT janela,
               CAST(floor((CAST(substr(janela, 1, 2) AS INT) * 60 + CAST(substr(janela, 4, 2) AS INT)
                           - {TEMPO_VIAGEM_MIN}) / 15) AS INT) AS faixa_id
        FROM VALUES {janelas_sql} AS v(janela)
    )
    SELECT e.estacao_nome, j.janela AS entrada_no_escritorio, f.inicio AS embarque,
           round(avg(p.entradas_previstas)) AS entradas_previstas_15min,
           round(avg(p.indice_lotacao), 2) AS indice_medio,
           CASE WHEN avg(p.indice_lotacao) < 0.35 THEN 'Vazio'
                WHEN avg(p.indice_lotacao) < 0.60 THEN 'Moderado'
                WHEN avg(p.indice_lotacao) < 0.85 THEN 'Cheio'
                ELSE 'Crítico' END AS categoria,
           rank() OVER (PARTITION BY e.estacao_nome ORDER BY avg(p.indice_lotacao)) AS ranking
    FROM janelas j
    JOIN {t('ouro_lotacao_prevista')} p ON p.faixa_id = j.faixa_id AND p.dia_semana_iso <= 5
    JOIN {t('dim_estacao')} e ON e.estacao_id = p.estacao_id
    JOIN {t('dim_faixa_horaria')} f ON f.faixa_id = j.faixa_id
    WHERE e.estacao_nome = '{estacao_escolhida}'
    GROUP BY e.estacao_nome, j.janela, f.inicio
    ORDER BY ranking
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** Para quem embarca em Corinthians-Itaquera, entrar no escritório às 10:30 (embarque às 09:30) é a melhor opção: a lotação prevista é "Vazio", com cerca de 450 entradas a cada 15 minutos, contra cerca de 2.500 para quem entra às 08:00 (embarque às 07:00, "Crítico"). Trocar 08:00 por 10:30 significa embarcar com cerca de 82% menos gente chegando à estação ao mesmo tempo. A 09:30 é a segunda melhor opção, e a 07:00 ainda é melhor que a 08:00.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 3: em qual dia da semana é mais tranquilo ir ao escritório?

# COMMAND ----------

display(spark.sql(f"""
    WITH total_dia AS (
        SELECT data, sum(entradas_estimadas) AS entradas
        FROM {t('fato_lotacao')}
        GROUP BY data
    ),
    por_dia_semana AS (
        SELECT d.dia_semana_iso, d.dia_semana_nome, count(*) AS dias, avg(x.entradas) AS media
        FROM total_dia x
        JOIN {t('dim_data')} d ON d.data = x.data
        WHERE d.tipo_dia = 'Dia útil'
        GROUP BY d.dia_semana_iso, d.dia_semana_nome
    )
    SELECT dia_semana_nome, dias,
           round(media) AS media_entradas_linhas_1_e_3,
           round(100 * (media / avg(media) OVER () - 1), 1) AS diferenca_para_a_media_pct,
           rank() OVER (ORDER BY media) AS ranking_mais_tranquilo
    FROM por_dia_semana
    ORDER BY dia_semana_iso
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** Nos dados reais de janeiro a setembro de 2026, a segunda-feira é o dia útil mais tranquilo (2,8% abaixo da média), seguida da sexta (0,8% abaixo). Terça e quarta são os dias mais cheios. A diferença entre os dias é pequena (menos de 5%), então para o funcionário o horário de entrada pesa muito mais que o dia da semana. Mesmo assim, uma empresa que concentra o presencial de terça a quinta coloca o time justamente nos dias mais cheios.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 4: quanto o movimento cai em feriados, emendas e fins de semana?

# COMMAND ----------

display(spark.sql(f"""
    WITH total_dia AS (
        SELECT data, sum(entradas_estimadas) AS entradas
        FROM {t('fato_lotacao')}
        GROUP BY data
    ),
    por_tipo AS (
        SELECT d.tipo_dia, count(*) AS dias, avg(x.entradas) AS media
        FROM total_dia x
        JOIN {t('dim_data')} d ON d.data = x.data
        GROUP BY d.tipo_dia
    )
    SELECT tipo_dia, dias,
           round(media) AS media_entradas_por_dia,
           round(100 * (media / max(CASE WHEN tipo_dia = 'Dia útil' THEN media END) OVER () - 1), 1)
               AS diferenca_para_dia_util_pct
    FROM por_tipo
    ORDER BY media DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** Em relação a um dia útil comum, o movimento cai 33% nas emendas, 39% aos sábados, 59% nos feriados e 63% aos domingos. As emendas são uma oportunidade: são dias úteis, com o Metrô um terço mais vazio, bons para dias presenciais.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 5 (impacto): quanto a escala sugerida tira os funcionários do pico?

# COMMAND ----------

display(spark.sql(f"""
    WITH resumo AS (
        SELECT CAST(count(*) AS DOUBLE) AS viagens,
               CAST(sum(CAST(pico_original AS INT)) AS DOUBLE) AS pico_original,
               CAST(sum(CAST(pico_sugerida AS INT)) AS DOUBLE) AS pico_sugerida,
               CAST(sum(CAST(categoria_original = 'Crítico' AS INT)) AS DOUBLE) AS critico_original,
               CAST(sum(CAST(categoria_sugerida = 'Crítico' AS INT)) AS DOUBLE) AS critico_sugerida,
               round(avg(indice_original), 2) AS indice_original,
               round(avg(indice_sugerida), 2) AS indice_sugerida,
               CAST(sum(CAST(chuva_original_mm > 0 AS INT)) AS DOUBLE) AS chuva_original,
               CAST(sum(CAST(chuva_sugerida_mm > 0 AS INT)) AS DOUBLE) AS chuva_sugerida
        FROM {t('fato_viagem_escala')}
    )
    SELECT indicador, original AS escala_original, sugerida AS escala_sugerida,
           round(100 * (sugerida - original) / nullif(original, 0), 1) AS variacao_pct
    FROM resumo
    LATERAL VIEW stack(5,
        'Viagens presenciais', viagens, viagens,
        'Viagens no horário de pico', pico_original, pico_sugerida,
        'Viagens em faixa crítica', critico_original, critico_sugerida,
        'Índice médio de lotação no embarque', indice_original, indice_sugerida,
        'Embarques com chuva', chuva_original, chuva_sugerida
    ) s AS indicador, original, sugerida
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação:** Nas 104 viagens presenciais de 02 a 11/11, a escala original (todos entrando às 08:00) colocava 100% dos embarques no horário de pico e em faixa crítica. A escala sugerida pelo ForaDoPico reduz as viagens no pico de 104 para 11 (-89%), zera as viagens em faixa crítica e reduz o índice médio de lotação no embarque de 0,98 para 0,33 (-66%). Também caem os embarques com chuva, de 13 para 11. Os ganhos usam o perfil horário estimado e o limite de 40% do time por janela, que mantém gente no escritório em todos os horários.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Uso pelo MVP (requisito 08)
# MAGIC O app lê **somente estas duas views**, que não têm nome, e-mail nem id do funcionário:
# MAGIC - `vw_app_lotacao_prevista`: tela **Lotação prevista** (também usada na versão gratuita).
# MAGIC - `vw_app_minha_semana`: tela **Minha semana** (agenda com a janela sugerida e a hora de sair de casa).

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('vw_app_lotacao_prevista')} AS
    SELECT e.linha_nome, e.estacao_nome, p.dia_semana_iso,
           element_at(array('Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'),
                      p.dia_semana_iso) AS dia_semana_nome,
           f.inicio AS faixa, p.entradas_previstas, p.indice_lotacao, p.categoria
    FROM {t('ouro_lotacao_prevista')} p
    JOIN {t('dim_estacao')} e ON e.estacao_id = p.estacao_id
    JOIN {t('dim_faixa_horaria')} f ON f.faixa_id = p.faixa_id
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('vw_app_minha_semana')} AS
    SELECT v.funcionario_chave, v.data, d.dia_semana_nome, v.time_nome, e.estacao_nome,
           v.janela_sugerida AS entrar_no_escritorio, v.embarque_sugerida AS embarcar_ate,
           v.categoria_sugerida AS lotacao_prevista, v.janela_original
    FROM {t('fato_viagem_escala')} v
    JOIN {t('dim_estacao')} e ON e.estacao_id = v.estacao_id
    JOIN {t('dim_data')} d ON d.data = v.data
""")

display(spark.sql(f"""
    SELECT * FROM {t('vw_app_lotacao_prevista')}
    WHERE estacao_nome = 'Corinthians-Itaquera' AND dia_semana_iso = 1 AND faixa BETWEEN '05:30' AND '10:00'
    ORDER BY faixa
"""))

# COMMAND ----------

display(spark.table(t("vw_app_minha_semana")).orderBy("data", "time_nome").limit(15))
