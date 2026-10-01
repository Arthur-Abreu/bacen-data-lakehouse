# Imagem base enxuta com Python 3.11
FROM python:3.11-slim

# PySpark depende de uma JVM instalada no sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia só o requirements primeiro para aproveitar cache do Docker
# (se o código mudar mas as dependências não, não reinstala tudo de novo)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Agora copia o resto do código
COPY ingest_bcb.py transform_silver.py ./

# Variáveis de ambiente que o boto3/Spark usam para achar as credenciais AWS
# (os valores reais vêm do docker-compose.yml ou do ambiente, nunca hardcoded aqui)
ENV AWS_DEFAULT_REGION=us-east-2 

# Comando padrão: roda a ingestão. Para rodar a transformação, veja o
# docker-compose.yml ou sobrescreva o comando (docker run ... python transform_silver.py)
CMD ["python", "ingest_bcb.py"]
