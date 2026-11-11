"""
job_bronze_silver_dimensoes  (Bronze -> Silver)  - Glue 5.0

Equivalente em script do job VISUAL com dois ramos independentes:
  Ramo 1: mro_bronze.produtos (JSON) -> silver/produtos (Parquet)
  Ramo 2: mro_bronze.avioes   (CSV)  -> silver/avioes   (Parquet)

As dimensoes chegam como "fotografias" (snapshots) completas. Com o Job Bookmark
ligado, um arquivo so e lido de novo se for substituido na Bronze; cada nova
fotografia recebe a coluna data_carga e a camada Gold usa sempre a mais recente.
Assim a Silver guarda historico e o pipeline continua idempotente.
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DB_BRONZE", "DB_SILVER"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
BUCKET = args["BUCKET"]


def gravar(dyf, tabela):
    sink = glueContext.getSink(
        path=f"s3://{BUCKET}/silver/{tabela}/", connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True,
        transformation_ctx=f"destino_{tabela}")
    sink.setCatalogInfo(catalogDatabase=args["DB_SILVER"], catalogTableName=tabela)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dyf)


# ---------- Ramo 1: produtos ----------
prod = glueContext.create_dynamic_frame.from_catalog(
    database=args["DB_BRONZE"], table_name="produtos", transformation_ctx="origem_produtos")
if prod.count() > 0:
    # NO: Change Schema (preco chega como TEXTO na origem -> double)
    prod = ApplyMapping.apply(frame=prod, mappings=[
        ("id", "int", "id_produto", "int"),
        ("product", "string", "nome_produto", "string"),
        ("category", "string", "categoria", "string"),
        ("price", "string", "preco_lista", "double"),
        ("available", "boolean", "disponivel", "boolean"),
    ], transformation_ctx="schema_produtos")
    # NO: Derived Column (chave_produto, data_carga)
    df = (prod.toDF()
          .withColumn("chave_produto", F.lower(F.trim("nome_produto")))
          .withColumn("data_carga", F.current_timestamp())
          .coalesce(1))
    gravar(DynamicFrame.fromDF(df, glueContext, "produtos"), "produtos")

# ---------- Ramo 2: avioes ----------
av = glueContext.create_dynamic_frame.from_catalog(
    database=args["DB_BRONZE"], table_name="avioes", transformation_ctx="origem_avioes")
if av.count() > 0:
    av = ApplyMapping.apply(frame=av, mappings=[
        ("code", "long", "codigo_aeronave", "int"),
        ("plane", "string", "modelo", "string"),
        ("manufacturer", "string", "fabricante", "string"),
    ], transformation_ctx="schema_avioes")
    df = av.toDF().withColumn("data_carga", F.current_timestamp()).coalesce(1)
    gravar(DynamicFrame.fromDF(df, glueContext, "avioes"), "avioes")

job.commit()
