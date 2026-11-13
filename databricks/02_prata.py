# Databricks notebook source
# MAGIC %md
# MAGIC # ForaDoPico · 02 · Camada Prata
# MAGIC
# MAGIC Transforma o dado bruto da Bronze em dado **limpo e confiável** (requisito 06):
# MAGIC
# MAGIC 1. **Tipos corretos:** texto vira data, número e booleano; a demanda passa de milhares para passageiros.
# MAGIC 2. **Nulos e inválidos tratados:** nomes de estação padronizados pelo cadastro oficial do Metrô; o que não tem conserto vai para a quarentena.
# MAGIC 3. **Regras de qualidade** com o total barrado por regra (`controle_qualidade_prata`).
# MAGIC 4. **MERGE pela chave:** cada registro aparece uma vez só, mesmo vindo em vários arquivos. Rodar de novo não insere nem altera nada.
# MAGIC
# MAGIC A Prata lê a Bronze inteira a cada execução; quem garante que não há duplicação é o MERGE.
# MAGIC
# MAGIC **Como usar:** clique em **Run all**. Depois clique em **Run all** de novo: na seção 6, a segunda execução deve mostrar 0 inseridos e 0 atualizados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e funções auxiliares

# COMMAND ----------

from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "workspace"
SCHEMA = "foradopico"


def t(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
EXECUCAO = spark.sql("SELECT current_timestamp() AS agora").first().agora

qualidade = []    # (tabela, regra, descricao, avaliados, barrados)
quarentena = []   # registros barrados, um DataFrame por regra
merges = []       # (tabela, linhas_no_lote, inseridos, atualizados, total_na_tabela)
tratamentos = []  # (tabela, tratamento, registros)


def tc(coluna: str, tipo: str):
    """Conversão segura de tipo: valor que não converte vira nulo (e é barrado pelas regras), em vez de quebrar."""
    return F.expr(f"try_cast(`{coluna}` AS {tipo})")


def chave_nome(coluna):
    """Nome sem acento, minúsculo e só com letras e números: 'Tatuapé' e 'tatuape ' viram 'tatuape'."""
    return F.regexp_replace(
        F.translate(F.lower(F.trim(coluna)), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"),
        "[^a-z0-9]",
        "",
    )


def mais_recente(df, chaves):
    """Cada coleta repete dados já coletados: fica só a versão do arquivo mais recente de cada chave."""
    janela = Window.partitionBy(*chaves).orderBy(F.col("arquivo_origem").desc(), F.col("carregado_em").desc())
    return df.withColumn("_ordem", F.row_number().over(janela)).filter("_ordem = 1").drop("_ordem")


def aplicar_regras(df, tabela, regras):
    """Aplica as regras em sequência. Cada registro é barrado pela primeira regra que violar e vai
    para a quarentena (sem nome e e-mail); o restante segue para o MERGE."""
    avaliados = df.count()
    restante = df
    for codigo, descricao, invalido in regras:
        condicao = F.coalesce(invalido, F.lit(True))  # regra que não pode ser avaliada (nulo) barra o registro
        barrados = restante.filter(condicao)
        qualidade.append((tabela, codigo, descricao, avaliados, barrados.count()))
        colunas = [c for c in barrados.columns if c not in ("nome", "email", "carregado_em")]
        quarentena.append(
            barrados.select(
                F.lit(tabela).alias("tabela"),
                F.lit(codigo).alias("regra"),
                F.to_json(F.struct(*colunas)).alias("registro"),
                F.col("arquivo_origem"),
            )
        )
        restante = restante.filter(~condicao)
    return restante


def com_hash(df, colunas_negocio):
    """Hash dos valores de negócio: o MERGE só atualiza um registro se o conteúdo dele mudou."""
    valores = [F.coalesce(F.col(c).cast("string"), F.lit("<nulo>")) for c in colunas_negocio]
    return df.withColumn("hash_registro", F.sha2(F.concat_ws("||", *valores), 256)).withColumn(
        "processado_em", F.current_timestamp()
    )


def merge_prata(df, tabela, chaves):
    """Upsert pela chave: insere o que é novo e só atualiza o que mudou."""
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(t(tabela))  # cria vazia na 1ª vez
    df.createOrReplaceTempView("lote_prata")
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    r = spark.sql(f"""
        MERGE INTO {t(tabela)} AS alvo
        USING lote_prata AS novo
        ON {condicao}
        WHEN MATCHED AND alvo.hash_registro <> novo.hash_registro THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    inseridos, atualizados = int(r["num_inserted_rows"]), int(r["num_updated_rows"])
    merges.append((tabela, df.count(), inseridos, atualizados, spark.table(t(tabela)).count()))
    print(f"{tabela}: {inseridos} inseridos, {atualizados} atualizados")


print("Configuração pronta. Execução:", EXECUCAO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Demanda do Metrô e cadastro de estações
# MAGIC Chave: `data + linha + estacao_codigo` (a linha entra na chave porque estações de baldeação, como a Sé, aparecem nas duas linhas).

# COMMAND ----------

bruto = spark.table(t("bronze_demanda_metro"))

demanda = mais_recente(
    bruto.select(
        tc("data", "DATE").alias("data"),
        tc("linha", "INT").alias("linha"),
        F.lower(F.trim("estacao_codigo")).alias("estacao_codigo"),
        F.trim("estacao_nome").alias("estacao_nome"),
        tc("entradas_mil", "DOUBLE").alias("entradas_mil"),
        tc("data_coleta", "DATE").alias("data_coleta"),
        "arquivo_origem",
        "carregado_em",
    ),
    ["data", "linha", "estacao_codigo"],
)
tratamentos.append(
    ("prata_demanda_metro", "Cópias repetidas entre coletas descartadas", bruto.count() - demanda.count())
)

demanda = aplicar_regras(demanda, "prata_demanda_metro", [
    ("R1", "Entradas nulas, não numéricas ou negativas",
     F.col("entradas_mil").isNull() | (F.col("entradas_mil") < 0)),
    ("R2", "Data inválida ou posterior à data da coleta",
     F.col("data").isNull() | (F.col("data") > F.col("data_coleta"))),
    ("R3", "Chave incompleta (linha ou estação ausente)",
     F.col("linha").isNull() | F.col("estacao_codigo").isNull()),
])

demanda = com_hash(
    demanda.select(
        "data",
        "linha",
        "estacao_codigo",
        "estacao_nome",
        F.round(F.col("entradas_mil") * 1000).cast("int").alias("entradas"),  # milhares -> passageiros
        "data_coleta",
        "arquivo_origem",
    ),
    ["estacao_nome", "entradas"],
)
merge_prata(demanda, "prata_demanda_metro", ["data", "linha", "estacao_codigo"])

# Cadastro de estações (linha, código e nome oficial), usado para validar e padronizar a escala
ultima = Window.partitionBy("linha", "estacao_codigo").orderBy(F.col("data").desc())
estacoes = com_hash(
    spark.table(t("prata_demanda_metro"))
    .withColumn("_ordem", F.row_number().over(ultima))
    .filter("_ordem = 1")
    .select("linha", "estacao_codigo", "estacao_nome", chave_nome(F.col("estacao_nome")).alias("chave_nome")),
    ["estacao_nome", "chave_nome"],
)
merge_prata(estacoes, "prata_estacao", ["linha", "estacao_codigo"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clima por hora
# MAGIC O Open-Meteo devolve listas paralelas (horas, temperaturas e chuvas): `arrays_zip` junta as listas e `explode` vira uma linha por hora.
# MAGIC Chave: `cidade + data_hora`.

# COMMAND ----------

bruto = spark.table(t("bronze_clima"))
if dict(bruto.dtypes).get("hourly") == "string":  # garantia caso o schema tenha sido inferido como texto
    bruto = bruto.withColumn(
        "hourly",
        F.from_json("hourly", "time ARRAY<STRING>, temperature_2m ARRAY<STRING>, precipitation ARRAY<STRING>"),
    )

clima = mais_recente(
    bruto.select(
        "arquivo_origem",
        "carregado_em",
        F.explode(
            F.arrays_zip("hourly.time", "hourly.temperature_2m", "hourly.precipitation").cast(
                "array<struct<momento:string,temperatura:string,chuva:string>>"
            )
        ).alias("h"),
    ).select(
        F.lit("São Paulo").alias("cidade"),
        F.expr("try_to_timestamp(h.momento, 'yyyy-MM-dd\\'T\\'HH:mm')").alias("data_hora"),
        F.expr("try_cast(h.temperatura AS DOUBLE)").alias("temperatura_c"),
        F.expr("try_cast(h.chuva AS DOUBLE)").alias("precipitacao_mm"),
        "arquivo_origem",
        "carregado_em",
    ),
    ["cidade", "data_hora"],
)

clima = aplicar_regras(clima, "prata_clima", [
    ("C1", "Data e hora inválidas", F.col("data_hora").isNull()),
    ("C2", "Temperatura ou chuva nula ou fora da faixa física (-5 a 45 °C, 0 a 150 mm/h)",
     F.col("temperatura_c").isNull() | ~F.col("temperatura_c").between(-5, 45)
     | F.col("precipitacao_mm").isNull() | ~F.col("precipitacao_mm").between(0, 150)),
])

clima = com_hash(
    clima.select(
        "cidade",
        "data_hora",
        F.to_date("data_hora").alias("data"),
        F.hour("data_hora").alias("hora"),
        "temperatura_c",
        "precipitacao_mm",
        (F.col("precipitacao_mm") > 0).alias("choveu"),
        "arquivo_origem",
    ),
    ["temperatura_c", "precipitacao_mm"],
)
merge_prata(clima, "prata_clima", ["cidade", "data_hora"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feriados
# MAGIC Cada coleta traz a lista do ano inteiro: fica um registro por data. Chave: `data`.

# COMMAND ----------

bruto = spark.table(t("bronze_feriados"))
feriados = mais_recente(
    bruto.select(
        tc("date", "DATE").alias("data"),
        F.trim("name").alias("nome_feriado"),
        F.trim("type").alias("tipo"),
        "arquivo_origem",
        "carregado_em",
    ),
    ["data"],
)
tratamentos.append(("prata_feriados", "Cópias repetidas entre coletas descartadas", bruto.count() - feriados.count()))

feriados = aplicar_regras(feriados, "prata_feriados", [
    ("F1", "Data inválida ou feriado sem nome",
     F.col("data").isNull() | F.col("nome_feriado").isNull() | (F.col("nome_feriado") == "")),
])

feriados = com_hash(
    feriados.select("data", "nome_feriado", "tipo", "arquivo_origem"),
    ["nome_feriado", "tipo"],
)
merge_prata(feriados, "prata_feriados", ["data"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escala presencial da empresa
# MAGIC - **De-para de empresa:** `EMP001` passa a ter nome, o mesmo usado nos lotes do robô RPA.
# MAGIC - **Estação padronizada:** o nome digitado pelo RH é comparado ao cadastro oficial sem acento e sem maiúsculas (ex.: `Tatuape` vira `Tatuapé`).
# MAGIC - Chave: `data + funcionario_id`. Nome e e-mail ainda existem aqui; a proteção LGPD acontece na Ouro.

# COMMAND ----------

empresas = com_hash(
    spark.createDataFrame([("EMP001", "Empresa Fictícia S.A.")], "empresa_id string, empresa_nome string"),
    ["empresa_nome"],
)
merge_prata(empresas, "prata_empresa", ["empresa_id"])

bruto = spark.table(t("bronze_escala"))
escala = mais_recente(
    bruto.select(
        tc("data", "DATE").alias("data"),
        F.upper(F.trim("empresa_id")).alias("empresa_id"),
        F.trim(F.col("time")).alias("time_nome"),
        F.upper(F.trim("funcionario_id")).alias("funcionario_id"),
        F.initcap(F.trim("nome")).alias("nome"),
        F.lower(F.trim("email")).alias("email"),
        tc("linha_embarque", "INT").alias("linha"),
        F.when(F.trim("estacao_embarque") == "", F.lit(None))
        .otherwise(F.trim("estacao_embarque"))
        .alias("estacao_informada"),
        F.coalesce(F.lower(F.trim("presencial")) == "true", F.lit(False)).alias("presencial"),
        F.trim("janela_entrada").alias("janela_entrada"),
        tc("data_coleta", "DATE").alias("data_coleta"),
        "arquivo_origem",
        "carregado_em",
    ),
    ["data", "funcionario_id"],
)

cadastro = spark.table(t("prata_estacao")).select(
    "linha", "chave_nome", "estacao_codigo", F.col("estacao_nome").alias("estacao_oficial")
)
escala = escala.withColumn("chave_nome", chave_nome(F.col("estacao_informada"))).join(
    cadastro, ["linha", "chave_nome"], "left"
)
tratamentos.append((
    "prata_escala",
    "Nomes de estação padronizados pelo cadastro oficial",
    escala.filter(
        F.col("estacao_oficial").isNotNull() & (F.col("estacao_informada") != F.col("estacao_oficial"))
    ).count(),
))

escala = aplicar_regras(escala, "prata_escala", [
    ("E1", "Estação de embarque vazia", F.col("estacao_informada").isNull()),
    ("E2", "Estação não encontrada no cadastro do Metrô (linha + nome)", F.col("estacao_oficial").isNull()),
    ("E3", "Horário de entrada inválido (fora de 00:00 a 23:59)",
     ~F.col("janela_entrada").rlike("^([01][0-9]|2[0-3]):[0-5][0-9]$")),
    ("E4", "Chave incompleta (data ou funcionário ausente)",
     F.col("data").isNull() | F.col("funcionario_id").isNull()),
])

escala = com_hash(
    escala.join(spark.table(t("prata_empresa")).select("empresa_id", "empresa_nome"), "empresa_id", "left").select(
        "data",
        "empresa_id",
        F.coalesce("empresa_nome", F.lit("Não cadastrada")).alias("empresa_nome"),
        "time_nome",
        "funcionario_id",
        "nome",
        "email",
        "linha",
        "estacao_codigo",
        F.col("estacao_oficial").alias("estacao_embarque"),
        "presencial",
        "janela_entrada",
        F.expr(
            "try_cast(substr(janela_entrada, 1, 2) AS INT) * 60 + try_cast(substr(janela_entrada, 4, 2) AS INT)"
        ).alias("minuto_entrada"),
        "data_coleta",
        "arquivo_origem",
    ),
    ["empresa_nome", "time_nome", "nome", "email", "linha", "estacao_codigo", "presencial", "janela_entrada"],
)
merge_prata(escala, "prata_escala", ["data", "funcionario_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências do requisito 06
# MAGIC ### 6.1 Regras de qualidade: total barrado por regra

# COMMAND ----------

regras = spark.createDataFrame(
    qualidade, "tabela string, regra string, descricao string, avaliados long, barrados long"
).withColumn("executado_em", F.lit(EXECUCAO))
regras.write.mode("append").saveAsTable(t("controle_qualidade_prata"))
display(regras.select("tabela", "regra", "descricao", "avaliados", "barrados").orderBy("tabela", "regra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Tratamentos aplicados

# COMMAND ----------

display(spark.createDataFrame(tratamentos, "tabela string, tratamento string, registros long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 MERGE pela chave (últimas 2 execuções)
# MAGIC Na 1ª execução tudo é inserido. Na 2ª, sem dado novo, o esperado é **0 inseridos e 0 atualizados**.

# COMMAND ----------

spark.createDataFrame(
    merges, "tabela string, linhas_no_lote long, inseridos long, atualizados long, total_na_tabela long"
).withColumn("executado_em", F.lit(EXECUCAO)).write.mode("append").saveAsTable(t("controle_merge_prata"))

display(spark.sql(f"""
    SELECT executado_em, tabela, linhas_no_lote, inseridos, atualizados, total_na_tabela
    FROM (
        SELECT *, dense_rank() OVER (ORDER BY executado_em DESC) AS rodada
        FROM {t('controle_merge_prata')}
    )
    WHERE rodada <= 2
    ORDER BY executado_em, tabela
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Quarentena: registros barrados (sem nome e e-mail)

# COMMAND ----------

(
    reduce(lambda a, b: a.unionByName(b), quarentena)
    .withColumn("barrado_em", F.lit(EXECUCAO))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(t("quarentena_prata"))
)
display(spark.table(t("quarentena_prata")).orderBy("tabela", "regra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Tipos das tabelas Prata

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name AS tabela, column_name AS coluna, data_type AS tipo
    FROM {CATALOGO}.information_schema.columns
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'prata_%'
      AND column_name NOT IN ('hash_registro', 'processado_em', 'arquivo_origem')
    ORDER BY table_name, ordinal_position
"""))

# COMMAND ----------

display(spark.table(t("prata_escala")).drop("nome", "email", "hash_registro").orderBy("data", "funcionario_id").limit(10))
