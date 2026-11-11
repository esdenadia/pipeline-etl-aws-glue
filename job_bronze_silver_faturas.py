"""
job_bronze_silver_faturas  (Bronze -> Silver)  - Glue 5.0

Equivalente em script do job VISUAL montado no Glue Studio (ver docs/GUIA_PASSO_A_PASSO.md).
Cada bloco abaixo corresponde a um no do editor visual (comentario "NO: ...").
Use este script apenas se preferir criar o job por codigo (parametro
UsarJobsVisuais=false na stack 02_orquestracao.yaml).

Recursos demonstrados:
  - Job Bookmark (transformation_ctx na origem)  -> processamento incremental
  - groupFiles/groupSize                          -> trata "muitos arquivos pequenos"
  - limpeza, padronizacao e quarentena            -> qualidade
  - hash SHA-256 do e-mail                        -> protecao de dado pessoal (LGPD)
  - Evaluate Data Quality (DQDL)                  -> validacao publicada no console/CloudWatch
  - repartition por ano/mes + Parquet Snappy      -> poucos arquivos, consulta barata
  - atualizacao automatica do Data Catalog        -> governanca de metadados
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DB_BRONZE", "DB_SILVER"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["BUCKET"]

DQ_RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "codigo_fatura",
    IsUnique "codigo_fatura",
    IsComplete "data_fatura",
    IsComplete "chave_produto",
    ColumnValues "preco_unitario" > 0,
    ColumnValues "quantidade" > 0,
    ColumnValues "quantidade" <= 100,
    ColumnValues "codigo_aeronave" <= 30 with threshold > 0.99,
    Completeness "responsavel" > 0.4
]
"""

# NO: Origem "AWS Glue Data Catalog" (mro_bronze.faturas) - bookmark + agrupamento de arquivos
origem = glueContext.create_dynamic_frame.from_catalog(
    database=args["DB_BRONZE"],
    table_name="faturas",
    transformation_ctx="origem_faturas",          # chave do Job Bookmark
    additional_options={"groupFiles": "inPartition", "groupSize": "134217728"},  # 128 MB
)

# Se o bookmark nao encontrou arquivos novos, encerra sem escrever nada (idempotencia)
if origem.count() == 0:
    print("Nenhum arquivo novo desde a ultima execucao (Job Bookmark). Nada a fazer.")
    job.commit()
    sys.exit(0)

# NO: Change Schema (renomeia para portugues e tipa as colunas)
tipado = ApplyMapping.apply(
    frame=origem,
    mappings=[
        ("code", "long", "codigo_fatura", "long"),
        ("date", "string", "data_fatura", "date"),
        ("item", "string", "item", "string"),
        ("price", "double", "preco_unitario", "double"),
        ("quantity", "int", "quantidade", "int"),
        ("plane_code", "int", "codigo_aeronave", "int"),
        ("created_at", "string", "criado_em", "timestamp"),
        ("responsible", "string", "responsavel", "string"),
        ("responsible_email", "string", "email_responsavel", "string"),
        ("description", "string", "descricao", "string"),
        ("ingest_date", "string", "data_ingestao", "string"),
    ],
    transformation_ctx="change_schema",
)
df = tipado.toDF()

# NO: Derived Column x3 (chave de juncao normalizada e colunas de particao)
df = (df.withColumn("chave_produto", F.lower(F.trim(F.col("item"))))
        .withColumn("ano", F.year("data_fatura"))
        .withColumn("mes", F.month("data_fatura")))

# NO: SQL Query "quarentena" -> registros invalidos vao para silver/quarentena_faturas
invalidos = df.where("preco_unitario IS NULL OR preco_unitario <= 0 OR quantidade IS NULL OR quantidade <= 0")
validos = df.where("preco_unitario > 0 AND quantidade > 0")

(invalidos.withColumn("motivo", F.when(F.col("preco_unitario").isNull(), "preco_nulo")
                                  .when(F.col("preco_unitario") <= 0, "preco_nao_positivo")
                                  .otherwise("quantidade_invalida"))
          .coalesce(1).write.mode("append").partitionBy("data_ingestao")
          .parquet(f"s3://{BUCKET}/silver/quarentena_faturas/"))

# NO: Drop Duplicates (chave: codigo_fatura) - remove reenvios da origem
validos = validos.dropDuplicates(["codigo_fatura"])

# NO: Detect Sensitive Data (EMAIL -> SHA-256). No visual: acao "Apply cryptographic hash".
validos = validos.withColumn(
    "email_responsavel",
    F.when(F.col("email_responsavel").isNotNull(), F.sha2(F.col("email_responsavel"), 256)))

# NO: Autobalance Processing (repartition pelas colunas de particao => ~1 arquivo por mes)
validos = validos.repartition("ano", "mes")
dyf = DynamicFrame.fromDF(validos, glueContext, "silver_faturas")

# NO: Evaluate Data Quality (publica resultados e metricas no CloudWatch)
EvaluateDataQuality.apply(
    frame=dyf,
    ruleset=DQ_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_silver_faturas",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# NO: Destino Amazon S3 - Parquet/Snappy, particionado, cria/atualiza tabela no Catalog
sink = glueContext.getSink(
    path=f"s3://{BUCKET}/silver/faturas/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["ano", "mes"],
    enableUpdateCatalog=True,
    transformation_ctx="destino_silver_faturas",
)
sink.setCatalogInfo(catalogDatabase=args["DB_SILVER"], catalogTableName="faturas")
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(dyf)

job.commit()  # grava o estado do bookmark somente se tudo deu certo
