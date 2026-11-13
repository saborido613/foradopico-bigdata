# Databricks notebook source
# MAGIC %md
# MAGIC # ForaDoPico · 01 · Camada Bronze
# MAGIC
# MAGIC Carrega os arquivos que o Airflow gravou em `chegada/` e foram enviados ao Volume em lotes.
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) com checkpoint: cada execução lê **só os arquivos novos**.
# MAGIC - Rodar de novo sem arquivos novos **não duplica nada** (requisito 05).
# MAGIC - A Bronze guarda o dado **bruto**, como veio da fonte, mais `arquivo_origem` e `carregado_em`.
# MAGIC - Cada execução registra as contagens em `controle_contagens_bronze` (evidência para o relatório).
# MAGIC
# MAGIC **Ordem de uso:** rode a célula de configuração → envie o lote 1 ao Volume → rode o resto com `etapa = 1_lote1`
# MAGIC → envie o lote 2 → rode com `etapa = 2_lote2` → rode de novo, sem enviar nada, com `etapa = 3_reexecucao`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração (schema, volumes e pastas)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
SCHEMA = "foradopico"
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada"     # onde os lotes são enviados
CONTROLE = f"/Volumes/{CATALOGO}/{SCHEMA}/controle"  # schemas e checkpoints do Auto Loader

# fonte -> (formato, opções de leitura)
FONTES = {
    "demanda_metro": ("csv", {"header": "true"}),
    "clima": ("json", {"multiLine": "true"}),
    "feriados": ("json", {"multiLine": "true"}),
    "escala": ("csv", {"header": "true"}),
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.chegada")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.controle")
for fonte in FONTES:
    dbutils.fs.mkdirs(f"{VOLUME}/{fonte}")

dbutils.widgets.text("etapa", "1_lote1", "Etapa (1_lote1, 2_lote2, 3_reexecucao)")
print("Pronto. Pastas criadas em", VOLUME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga incremental com Auto Loader
# MAGIC Só rode depois de enviar um lote ao Volume (com a pasta vazia o Auto Loader não consegue inferir o schema).

# COMMAND ----------

def carregar_bronze(fonte: str) -> None:
    formato, opcoes = FONTES[fonte]
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{fonte}")
    )
    for chave, valor in opcoes.items():
        leitor = leitor.option(chave, valor)

    consulta = (
        leitor.load(f"{VOLUME}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{CONTROLE}/checkpoint_{fonte}")
        .trigger(availableNow=True)  # processa o que é novo e para
        .toTable(f"{CATALOGO}.{SCHEMA}.bronze_{fonte}")
    )
    consulta.awaitTermination()
    print(f"bronze_{fonte}: carga concluída")


for fonte in FONTES:
    carregar_bronze(fonte)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contagens (prints do requisito 05 e do item 07 do relatório)

# COMMAND ----------

etapa = dbutils.widgets.get("etapa")

medidas = []
for fonte in FONTES:
    r = spark.sql(f"""
        SELECT COUNT(*) AS registros, COUNT(DISTINCT arquivo_origem) AS arquivos
        FROM {CATALOGO}.{SCHEMA}.bronze_{fonte}
    """).first()
    medidas.append((etapa, f"bronze_{fonte}", r.registros, r.arquivos))

(
    spark.createDataFrame(medidas, "etapa string, tabela string, registros long, arquivos long")
    .withColumn("medido_em", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(f"{CATALOGO}.{SCHEMA}.controle_contagens_bronze")
)

display(
    spark.table(f"{CATALOGO}.{SCHEMA}.controle_contagens_bronze")
    .orderBy("tabela", "medido_em")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prova de que não há arquivo duplicado
# MAGIC Cada arquivo deve aparecer uma única vez em cada tabela, com uma única data de carga.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'bronze_escala' AS tabela, arquivo_origem,
           COUNT(DISTINCT carregado_em) AS cargas, COUNT(*) AS registros
    FROM {CATALOGO}.{SCHEMA}.bronze_escala
    GROUP BY arquivo_origem
    ORDER BY arquivo_origem
"""))
