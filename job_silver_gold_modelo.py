"""
job_silver_gold_modelo  (Silver -> Gold)  - Glue 5.0

Constroi o modelo dimensional (esquema estrela) consumido pelo Athena:

    dim_tempo ─┐
    dim_produto ─┼── fato_faturas  (grao: 1 linha por fatura)
    dim_aeronave ┘
    agg_gastos_mensais (agregado pronto para BI: mes x fabricante x categoria)

Idempotencia: a Gold e RECALCULADA a partir da Silver e gravada com
mode("overwrite") + partitionOverwriteMode=dynamic. Rodar o job 1 ou 10 vezes
com a mesma Silver produz exatamente o mesmo resultado (sem duplicar linhas).
Configuravel por parametros (--BUCKET, --DB_SILVER, --PREFIXO_GOLD).

A funcao construir_modelo() nao depende do Glue e e testada localmente com
PySpark (ver testes/teste_local.py).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DESCONHECIDO = -1  # chave substituta para membros nao encontrados (integridade referencial)


def ultima_foto(df: DataFrame, chave: str) -> DataFrame:
    """Mantem apenas o snapshot mais recente de cada membro da dimensao."""
    w = Window.partitionBy(chave).orderBy(F.col("data_carga").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def construir_modelo(faturas: DataFrame, produtos: DataFrame, avioes: DataFrame, spark: SparkSession):
    # ---------------- Dimensao Produto ----------------
    p = ultima_foto(produtos, "id_produto")
    dim_produto = p.select(
        F.col("id_produto").alias("sk_produto"),
        "chave_produto", "nome_produto", "categoria", "preco_lista", "disponivel")
    dim_produto = dim_produto.unionByName(spark.createDataFrame(
        [(DESCONHECIDO, "desconhecido", "Produto não identificado", "Não identificada", None, None)],
        dim_produto.schema))

    # ---------------- Dimensao Aeronave ----------------
    a = ultima_foto(avioes, "codigo_aeronave")
    dim_aeronave = a.select(
        F.col("codigo_aeronave").alias("sk_aeronave"), "modelo", "fabricante",
        F.when(F.col("fabricante").rlike("Lockheed|Boeing|Northrop|General Dynamics|Grumman|McDonnell"),
               "EUA").otherwise("Outros").alias("origem_fabricante"))
    dim_aeronave = dim_aeronave.unionByName(spark.createDataFrame(
        [(DESCONHECIDO, "Aeronave não identificada", "Não identificado", "Não identificado")],
        dim_aeronave.schema))

    # ---------------- Dimensao Tempo (gerada a partir do intervalo dos fatos) ----------------
    limites = faturas.agg(F.min("data_fatura").alias("ini"), F.max("data_fatura").alias("fim"))
    dim_tempo = (limites.select(F.explode(F.sequence("ini", "fim")).alias("data"))
                 .select(
                     F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
                     "data",
                     F.year("data").alias("ano"),
                     F.quarter("data").alias("trimestre"),
                     F.month("data").alias("mes"),
                     F.dayofmonth("data").alias("dia"),
                     F.date_format("data", "yyyy-MM").alias("ano_mes"),
                     F.dayofweek("data").alias("dia_semana_num"),
                     F.dayofweek("data").isin(1, 7).alias("fim_de_semana")))

    # ---------------- Fato Faturas ----------------
    chaves_prod = dim_produto.select("sk_produto", "chave_produto").where(f"sk_produto <> {DESCONHECIDO}")
    chaves_av = dim_aeronave.select("sk_aeronave").where(f"sk_aeronave <> {DESCONHECIDO}")
    fato = (faturas.alias("f")
            .join(F.broadcast(chaves_prod).alias("p"), "chave_produto", "left")
            .join(F.broadcast(chaves_av).alias("a"),
                  F.col("f.codigo_aeronave") == F.col("a.sk_aeronave"), "left")
            .select(
                "f.codigo_fatura",
                F.date_format("f.data_fatura", "yyyyMMdd").cast("int").alias("sk_data"),
                F.coalesce("p.sk_produto", F.lit(DESCONHECIDO)).alias("sk_produto"),
                F.coalesce("a.sk_aeronave", F.lit(DESCONHECIDO)).alias("sk_aeronave"),
                "f.quantidade",
                "f.preco_unitario",
                F.round(F.col("f.quantidade") * F.col("f.preco_unitario"), 2).alias("valor_total"),
                "f.responsavel",
                F.col("f.email_responsavel").alias("email_responsavel_hash"),
                "f.criado_em",
                "f.ano", "f.mes"))

    # ---------------- Agregado mensal para BI ----------------
    agg = (fato.join(F.broadcast(dim_produto.select("sk_produto", "categoria")), "sk_produto")
               .join(F.broadcast(dim_aeronave.select("sk_aeronave", "fabricante")), "sk_aeronave")
               .groupBy("ano", "mes", "fabricante", "categoria")
               .agg(F.countDistinct("codigo_fatura").alias("qtd_faturas"),
                    F.sum("quantidade").alias("qtd_itens"),
                    F.round(F.sum("valor_total"), 2).alias("valor_total"),
                    F.round(F.avg("valor_total"), 2).alias("ticket_medio")))

    return {"dim_produto": dim_produto, "dim_aeronave": dim_aeronave, "dim_tempo": dim_tempo,
            "fato_faturas": fato, "agg_gastos_mensais": agg}


def gravar_modelo(tabelas: dict, base: str):
    """Escrita idempotente em Parquet/Snappy com poucos arquivos por particao."""
    for nome, df in tabelas.items():
        destino = f"{base}/{nome}/"
        w = df
        if nome == "fato_faturas":
            w = df.repartition("ano", "mes").write.partitionBy("ano", "mes")
        elif nome == "agg_gastos_mensais":
            w = df.coalesce(1).write.partitionBy("ano")
        else:
            w = df.coalesce(1).write
        w.mode("overwrite").option("compression", "snappy").parquet(destino)


if __name__ == "__main__":
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DB_SILVER", "PREFIXO_GOLD"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    db = args["DB_SILVER"]
    ler = lambda t: glueContext.create_dynamic_frame.from_catalog(
        database=db, table_name=t, transformation_ctx=f"src_{t}").toDF()
    modelo = construir_modelo(ler("faturas"), ler("produtos"), ler("avioes"), spark)
    gravar_modelo(modelo, f"s3://{args['BUCKET']}/{args['PREFIXO_GOLD']}")
    # As tabelas da Gold sao registradas/atualizadas pelo crawler_mro_gold (proximo passo do workflow)
    job.commit()
