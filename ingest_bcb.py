"""
Ingestão de séries temporais do Banco Central (API SGS) para a camada
bronze (raw) de um Data Lake no S3.

API pública do BC, sem necessidade de autenticação:
https://api.bcb.gov.br/dados/serie/bcdata.sgs.<codigo>/dados?formato=json

Séries usadas como exemplo (ajuste conforme o que quiser acompanhar):
    - 432  -> Meta Selic definida pelo Copom (% a.a.)
    - 433  -> IPCA - variação mensal (%)
    - 1    -> Dólar comercial - venda (PTAX)
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

# Nome do bucket S3 onde a camada raw/bronze vai ser gravada.
# Troque pelo nome do bucket que você criar na sua conta AWS.
BUCKET_NAME = "arthur-aws-bcb-data-lake"


@dataclass
class Serie:
    codigo: int
    nome: str


SERIES = [
    Serie(432, "selic_meta"),
    Serie(433, "ipca_mensal"),
    Serie(1, "dolar_ptax"),
]


def buscar_serie(serie: Serie, anos: int = 5) -> list[dict]:
    """Busca os últimos N anos de uma série do SGS/BCB.

    Desde março de 2025, o endpoint '/ultimos/N' passou a aceitar no
    máximo 20 registros, então usamos o endpoint por intervalo de datas
    (limitado a 10 anos por consulta) para trazer um histórico real.
    """
    hoje = datetime.now(timezone.utc)
    data_final = hoje.strftime("%d/%m/%Y")
    data_inicial = hoje.replace(year=hoje.year - anos).strftime("%d/%m/%Y")

    url = BCB_BASE_URL.format(codigo=serie.codigo)
    params = {
        "formato": "json",
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }

    logger.info(
        "Buscando série %s (%s) de %s até %s", serie.nome, serie.codigo, data_inicial, data_final
    )
    resposta = requests.get(url, params=params, timeout=30)
    resposta.raise_for_status()

    dados = resposta.json()
    logger.info("Série %s: %d registros recebidos", serie.nome, len(dados))
    return dados


def salvar_no_s3(serie: Serie, dados: list[dict]) -> str:
    """Salva o payload bruto no S3, particionado por data de ingestão."""
    s3 = boto3.client("s3")

    hoje = datetime.now(timezone.utc)
    chave = (
        f"raw/bcb/{serie.nome}/"
        f"ano={hoje:%Y}/mes={hoje:%m}/dia={hoje:%d}/"
        f"{serie.nome}_{hoje:%Y%m%dT%H%M%S}.json"
    )

    corpo = json.dumps(
        {
            "serie_codigo": serie.codigo,
            "serie_nome": serie.nome,
            "coletado_em": hoje.isoformat(),
            "registros": dados,
        },
        ensure_ascii=False,
    )

    s3.put_object(Bucket=BUCKET_NAME, Key=chave, Body=corpo.encode("utf-8"))
    logger.info("Gravado em s3://%s/%s", BUCKET_NAME, chave)
    return chave


def main() -> None:
    for serie in SERIES:
        try:
            dados = buscar_serie(serie)
            salvar_no_s3(serie, dados)
        except requests.HTTPError as exc:
            logger.error("Falha ao buscar série %s: %s", serie.nome, exc)
        except Exception as exc:  # noqa: BLE001 - log amplo para um script de ingestão simples
            logger.exception("Erro inesperado processando série %s: %s", serie.nome, exc)


if __name__ == "__main__":
    main()
