"""
job_simula_alteracoes_rds  - Glue 5.0

Simula o dia a dia do sistema de cadastros (a "origem" do CDC):
  - INSERT: 3 produtos novos (ids 61, 62 e 63)
  - UPDATE: reajuste de preco dos produtos 1 e 2
  - DELETE: o produto 60 sai do cadastro

E idempotente: rodar de novo deixa a tabela no mesmo estado final.
A tabela e pequena (cerca de 60 linhas), entao o job le tudo para a memoria,
aplica as mudancas e regrava a tabela no PostgreSQL pela conexao JDBC.
"""
import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONEXAO"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

props = boto3.client("glue").get_connection(Name=args["CONEXAO"])["Connection"]["ConnectionProperties"]
segredo = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=props["SECRET_ID"])["SecretString"])
url = props["JDBC_CONNECTION_URL"]
opcoes = {"user": segredo["username"], "password": segredo["password"], "driver": "org.postgresql.Driver"}

atual = spark.read.jdbc(url=url, table="public.produtos", properties=opcoes)
colunas = atual.columns
linhas = {r["id"]: r.asDict() for r in atual.collect()}  # leitura completa ANTES de regravar
print(f"Antes: {len(linhas)} produtos")

# INSERT
novos = [
    (61, "Torquimetro digital", "Ferramentas", "890.00", True),
    (62, "Kit de vedacao hidraulica", "Consumiveis", "145.90", True),
    (63, "Lanterna de inspecao LED", "Ferramentas", "210.00", True),
]
for i, nome, cat, preco, disp in novos:
    linhas[i] = {"id": i, "product": nome, "category": cat, "price": preco, "available": disp}
# UPDATE (reajuste de preco)
linhas[1]["price"] = "1350.00"
linhas[2]["price"] = "82.50"
# DELETE
linhas.pop(60, None)

depois = spark.createDataFrame([[l[c] for c in colunas] for l in linhas.values()], atual.schema)
(depois.coalesce(1).write.mode("overwrite").option("truncate", "true")
       .jdbc(url=url, table="public.produtos", properties=opcoes))
print(f"Depois: {len(linhas)} produtos (3 inseridos, 2 alterados, 1 excluido)")

job.commit()
