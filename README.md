# Pipeline de Dados Macroeconômicos do Banco Central (BCB)

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/apache_spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=for-the-badge&logo=terraform&logoColor=white)
![CI](https://github.com/Arthur-Abreu/bacen-data-lakehouse/actions/workflows/ci.yml/badge.svg)


Pipeline de dados em AWS que ingere séries temporais públicas do Banco
Central do Brasil (Selic, IPCA, câmbio), transforma com PySpark e
disponibiliza em camadas de Data Lake (arquitetura medalhão).

Projeto de portfólio voltado a vagas de Engenharia de Dados Júnior,
com foco em bancos digitais / fintechs.

## Por que esse projeto

Dados macroeconômicos (Selic, inflação, câmbio) são exatamente o tipo de
informação que áreas de risco, crédito e produto de uma fintech acompanham
no dia a dia. A proposta aqui é simular, em pequena escala, o tipo de
pipeline que sustenta esse tipo de análise.

## Arquitetura (medalhão)

```
API SGS (Banco Central)
        │
        ▼
   [ingest_bcb.py]  ──────────────▶  S3 - camada BRONZE (raw)
                                      JSON bruto, particionado por data
        │
        ▼
 [transform_silver.py - PySpark] ──▶  S3 - camada SILVER
                                      Parquet, tratado, com variação %
                                      e média móvel
        │
        ▼
      (próximo passo: camada GOLD + Athena)
```

## Decisões técnicas

- **Por que S3 em camadas (bronze/silver/gold) e não um banco relacional
  direto?** Mantém o dado bruto sempre disponível para reprocessamento,
  caso uma regra de transformação mude no futuro — prática comum em Data
  Lakes.
- **Por que PySpark, já que o volume de dados é pequeno?** O volume real
  do Banco Central não exige Spark, mas o objetivo aqui também é
  demonstrar domínio da ferramenta mais usada em vagas de engenharia de
  dados em bancos digitais, onde o mesmo código escalaria para volumes
  muito maiores sem mudanças estruturais.
- **Por que boto3 para ler/gravar no S3, em vez do Spark acessar o S3
  diretamente (s3a://)?** O conector S3A do Hadoop depende de um
  alinhamento fino entre várias versões de bibliotecas (hadoop-aws,
  aws-java-sdk-bundle, hadoop-common) que, na prática, é uma fonte comum
  de bugs de compatibilidade. Para o volume de dados deste projeto,
  baixar os arquivos da bronze localmente via boto3, processar com Spark
  local e subir o resultado de volta via boto3 é uma solução mais simples
  e mais estável, isolando o Spark do que ele faz melhor (processamento)
  e deixando a comunicação com o S3 a cargo de uma biblioteca mais madura
  para esse fim.
- **Por que a API SGS e não um dataset estático?** Permite ingestão
  recorrente de verdade (o BC atualiza as séries periodicamente), o que
  justifica a existência de uma camada de orquestração (ver próximos
  passos).
- **Idempotência na Camada Silver:** O script PySpark foi desenhado para 
  realizar o overwrite seguro dos dados. A partição de destino no S3 é 
  limpa antes do upload, evitando a duplicação de arquivos .parquet em 
  casos de reprocessamento e garantindo a confiabilidade da esteira de dados.

## Infraestrutura como código

O bucket S3 (com versionamento, bloqueio de acesso público e política de
ciclo de vida) e a role IAM do pipeline são provisionados via Terraform,
em vez de criados manualmente pelo console da AWS — garante que o ambiente
é reproduzível e versionado junto com o código.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars com o nome do seu bucket (precisa ser único na AWS)

terraform init
terraform plan
terraform apply
```

Para derrubar os recursos e evitar custo quando não estiver em uso:

```bash
terraform destroy
```

## CI/CD

A cada push ou pull request, o GitHub Actions roda automaticamente:
- Lint do código Python (ruff)
- `terraform fmt` e `terraform validate` na infraestrutura

Ver `.github/workflows/ci.yml`. Propositalmente, o workflow **não** roda
`terraform apply` automaticamente — mudança de infraestrutura é aplicada
manualmente, para evitar custo ou alteração indesejada na conta AWS.

## Rodando com Docker

O projeto roda em container, para não depender de instalar Java/Spark
diretamente na sua máquina.

```bash
cp .env.example .env
# edite o .env com o nome do seu bucket

docker compose build

# Rodar a ingestão (API BCB -> S3 bronze)
docker compose run --rm ingest

# Rodar a transformação PySpark (bronze -> S3 silver)
docker compose run --rm transform
```

As credenciais AWS da sua máquina (`~/.aws`) são montadas dentro do
container como somente leitura — assim o container consegue autenticar
na AWS sem que você precise copiar chaves de acesso para dentro da imagem.

## Como rodar (sem Docker)

```bash
pip install -r requirements.txt

# Configurar credenciais AWS (aws configure, ou variáveis de ambiente)
python ingest_bcb.py

# Transformação (requer Java + PySpark instalados localmente)
python transform_silver.py
```

## Próximos passos

- [ ] Orquestração com Apache Airflow (DAG diária de ingestão + transformação)
- [ ] Camada GOLD com métricas agregadas (ex: correlação Selic x inadimplência)
- [ ] Testes de qualidade de dados (valores dentro de ranges plausíveis, sem duplicatas)
- [ ] Catalogação via AWS Glue Data Catalog + consultas via Athena
- [x] Infraestrutura como código (Terraform) para provisionar o S3 e as roles IAM
- [ ] Projeto complementar de streaming (Kafka/Kinesis) simulando eventos de transação
- [x] CI/CD básico (GitHub Actions) rodando lint e validação a cada push

## Acompanhamento do projeto

O progresso é organizado em um board Kanban no GitHub Projects
(https://github.com/users/Arthur-Abreu/projects/1/views/1), com tarefas divididas em ciclos semanais.
