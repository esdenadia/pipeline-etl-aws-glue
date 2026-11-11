"""
job_cdc_produtos  - Glue 5.0  (CDC: origem JDBC -> Silver)

Captura de mudancas (CDC) por comparacao de fotografias:
  1. le o cadastro ATUAL no PostgreSQL (tabela JDBC catalogada em mro_rds);
  2. le a ULTIMA fotografia de cada produto na Silver (mro_silver.produtos);
  3. compara pela chave id_produto e classifica cada diferenca:
       I = insert (existe na origem e nao na Silver)
       U = update (existe nos dois, mas algum atributo mudou)
       D = delete (existe na Silver e sumiu da origem)
  4. grava o log de mudancas em silver/cdc_produtos (tabela mro_silver.cdc_produtos);
  5. aplica as mudancas na Silver acrescentando uma nova fotografia so dos
     produtos alterados. A Gold ja usa sempre a fotografia mais recente.

Exclusao logica: um produto excluido continua na dimensao (faturas antigas
apontam para ele), mas recebe disponivel = false.

Sem mudancas, o job termina sem gravar nada (idempotente).
Em producao, um CDC baseado no log do banco usaria o AWS DMS; aqui a comparacao
de fotografias atende porque o cadastro e pequeno.
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DB_ORIGEM", "TABELA_ORIGEM", "DB_SILVER"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
BUCKET = args["BUCKET"]
ATRIBUTOS = ["nome_produto", "categoria", "preco_lista", "disponivel"]


def gravar(df, tabela):
    sink = glueContext.getSink(
        path=f"s3://{BUCKET}/silver/{tabela}/", connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True,
        transformation_ctx=f"destino_{tabela}")
    sink.setCatalogInfo(catalogDatabase=args["DB_SILVER"], catalogTableName=tabela)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(DynamicFrame.fromDF(df.coalesce(1), glueContext, tabela))


# 1. Origem (cadastro atual, via conexao JDBC)
origem = (glueContext.create_dynamic_frame.from_catalog(
              database=args["DB_ORIGEM"], table_name=args["TABELA_ORIGEM"],
              transformation_ctx="origem_cdc").toDF()
          .select(F.col("id").cast("int").alias("id_produto"),
                  F.col("product").alias("nome_produto"),
                  F.col("category").alias("categoria"),
                  F.round(F.col("price").cast("double"), 2).alias("preco_lista"),
                  F.col("available").cast("boolean").alias("disponivel")))

# 2. Ultima fotografia de cada produto na Silver
silver = glueContext.create_dynamic_frame.from_catalog(
    database=args["DB_SILVER"], table_name="produtos", transformation_ctx="silver_cdc").toDF()
w = Window.partitionBy("id_produto").orderBy(F.col("data_carga").desc())
atual = (silver.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
         .select("id_produto", *ATRIBUTOS)
         .withColumn("preco_lista", F.round("preco_lista", 2)))

# 3. Comparacao e classificacao I / U / D
assinatura = lambda p: F.sha2(F.concat_ws("|", *[F.coalesce(F.col(f"{p}.{c}").cast("string"), F.lit("~"))
                                                 for c in ATRIBUTOS]), 256)
j = origem.alias("o").join(atual.alias("s"), "id_produto", "full_outer")
op = (F.when(F.col("s.nome_produto").isNull() & F.col("o.nome_produto").isNotNull(), "I")
       .when(F.col("o.nome_produto").isNull() & F.coalesce(F.col("s.disponivel"), F.lit(True)), "D")
       .when(F.col("o.nome_produto").isNotNull() & (assinatura("o") != assinatura("s")), "U"))
agora = F.current_timestamp()
mudancas = (j.withColumn("operacao", op).where(F.col("operacao").isNotNull())
            .select("id_produto", "operacao",
                    F.coalesce("o.nome_produto", "s.nome_produto").alias("nome_produto"),
                    F.coalesce("o.categoria", "s.categoria").alias("categoria"),
                    F.coalesce("o.preco_lista", "s.preco_lista").alias("preco_lista"),
                    F.when(F.col("operacao") == "D", F.lit(False)).otherwise(F.col("o.disponivel")).alias("disponivel"),
                    F.col("s.preco_lista").alias("preco_anterior"),
                    agora.alias("data_captura"))
            .cache())

resumo = {r["operacao"]: r["count"] for r in mudancas.groupBy("operacao").count().collect()}
print(f"CDC produtos -> inserts: {resumo.get('I', 0)} | updates: {resumo.get('U', 0)} | deletes: {resumo.get('D', 0)}")

if resumo:
    # 4. Log de mudancas
    gravar(mudancas, "cdc_produtos")
    # 5. Nova fotografia so dos produtos alterados (mesmo schema da Silver)
    foto = mudancas.select("id_produto", *ATRIBUTOS,
                           F.lower(F.trim("nome_produto")).alias("chave_produto"),
                           F.col("data_captura").alias("data_carga"))
    gravar(foto, "produtos")
else:
    print("Nenhuma mudanca na origem desde a ultima carga.")

job.commit()
