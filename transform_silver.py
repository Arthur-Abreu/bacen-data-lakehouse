"""
Transformação bronze -> silver usando PySpark.

Em vez de o Spark acessar o S3 diretamente (s3a://), o que depende de
bibliotecas do Hadoop-AWS com histórico de incompatibilidade de versões
entre si, este script usa boto3 (já usado no ingest_bcb.py) para baixar
os arquivos brutos para uma pasta local temporária, roda o Spark só
sobre arquivos locais, e sobe o resultado de volta para o S3 também via
boto3. Para o volume de dados deste projeto, isso é mais simples e mais
robusto do que depender do conector S3A.

Gera, para cada série:
    - valor numérico tratado (o SGS retorna como string)
    - variação percentual em relação ao registro anterior
    - média móvel de 3 períodos
"""

import logging
import tempfile
from pathlib import Path

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BUCKET_NAME = "arthur-aws-bcb-data-lake"
PREFIXO_BRONZE = "raw/bcb/"
PREFIXO_SILVER = "silver/bcb/"


def baixar_bronze(s3, pasta_local: Path) -> int:
    """Baixa todos os JSONs da camada bronze para uma pasta local."""
    paginador = s3.get_paginator("list_objects_v2")
    total = 0

    for pagina in paginador.paginate(Bucket=BUCKET_NAME, Prefix=PREFIXO_BRONZE):
        for obj in pagina.get("Contents", []):
            chave = obj["Key"]
            if not chave.endswith(".json"):
                continue

            destino = pasta_local / Path(chave).name
            s3.download_file(BUCKET_NAME, chave, str(destino))
            total += 1
            logger.info("Baixado: %s", chave)

    return total


def limpar_silver(s3) -> None:
    """Remove os arquivos antigos da camada silver no S3 antes de subir os novos.

    Necessário porque cada execução do Spark gera nomes de arquivo com um
    UUID diferente — sem essa limpeza, o S3 apenas acumularia parquet
    duplicado a cada execução, em vez de sobrescrever de fato.
    """
    paginador = s3.get_paginator("list_objects_v2")
    chaves_para_apagar = []

    for pagina in paginador.paginate(Bucket=BUCKET_NAME, Prefix=PREFIXO_SILVER):
        for obj in pagina.get("Contents", []):
            chaves_para_apagar.append({"Key": obj["Key"]})

    if not chaves_para_apagar:
        return

    # delete_objects aceita no máximo 1000 chaves por chamada
    for i in range(0, len(chaves_para_apagar), 1000):
        lote = chaves_para_apagar[i : i + 1000]
        s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": lote})

    logger.info("Removidos %d arquivo(s) antigos da camada silver", len(chaves_para_apagar))


def subir_silver(s3, pasta_local: Path) -> None:
    """Sobe os arquivos Parquet gerados de volta para o S3."""
    for arquivo in pasta_local.rglob("*"):
        if arquivo.is_file():
            chave_relativa = arquivo.relative_to(pasta_local).as_posix()
            chave = f"{PREFIXO_SILVER}{chave_relativa}"
            s3.upload_file(str(arquivo), BUCKET_NAME, chave)
            logger.info("Enviado: s3://%s/%s", BUCKET_NAME, chave)


def transformar(spark: SparkSession, pasta_bronze: Path, pasta_silver: Path) -> None:
    logger.info("Lendo camada bronze local em %s", pasta_bronze)

    df_bruto = spark.read.option("multiline", "true").json(str(pasta_bronze))

    # Cada arquivo bronze tem um array "registros" com {data, valor}; explode para uma linha por registro
    df_explodido = df_bruto.select(
        "serie_nome",
        "serie_codigo",
        F.explode("registros").alias("registro"),
    ).select(
        "serie_nome",
        "serie_codigo",
        F.to_date(F.col("registro.data"), "dd/MM/yyyy").alias("data"),
        F.col("registro.valor").cast("double").alias("valor"),
    )

    janela = Window.partitionBy("serie_nome").orderBy("data")

    df_silver = (
        df_explodido.dropDuplicates(["serie_nome", "data"])
        .withColumn("valor_anterior", F.lag("valor").over(janela))
        .withColumn(
            "variacao_percentual",
            F.round(
                (F.col("valor") - F.col("valor_anterior")) / F.col("valor_anterior") * 100, 4
            ),
        )
        .withColumn(
            "media_movel_3",
            F.round(F.avg("valor").over(janela.rowsBetween(-2, 0)), 4),
        )
    )

    logger.info("Gravando camada silver local em %s", pasta_silver)
    df_silver.write.mode("overwrite").partitionBy("serie_nome").parquet(str(pasta_silver))

    df_silver.show(20, truncate=False)


def main() -> None:
    s3 = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmp:
        pasta_bronze = Path(tmp) / "bronze"
        pasta_silver = Path(tmp) / "silver"
        pasta_bronze.mkdir()

        qtd = baixar_bronze(s3, pasta_bronze)
        if qtd == 0:
            logger.warning("Nenhum arquivo encontrado em s3://%s/%s", BUCKET_NAME, PREFIXO_BRONZE)
            return

        logger.info("%d arquivo(s) baixado(s) da camada bronze", qtd)

        spark = SparkSession.builder.appName("bcb-bronze-to-silver").getOrCreate()
        try:
            transformar(spark, pasta_bronze, pasta_silver)
        finally:
            spark.stop()

        logger.info("Removendo arquivos antigos da camada silver, se houver")
        limpar_silver(s3)

        subir_silver(s3, pasta_silver)
        logger.info("Transformação concluída com sucesso.")


if __name__ == "__main__":
    main()
