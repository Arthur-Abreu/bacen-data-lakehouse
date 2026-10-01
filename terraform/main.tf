terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Bucket S3 que vai hospedar as camadas bronze/silver/gold do Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Projeto       = "bcb-data-pipeline"
    Ambiente      = var.ambiente
    GerenciadoPor = "terraform"
  }
}

# Bloqueia qualquer acesso público ao bucket (boa prática de segurança)
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versionamento habilitado: permite recuperar uma versão anterior de um
# arquivo caso uma transformação com bug sobrescreva dados por engano
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Política de ciclo de vida: dados da camada bronze mais antigos que 90 dias
# vão para uma classe de armazenamento mais barata (Glacier Instant Retrieval)
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-para-glacier"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# Role IAM que o script de ingestão/transformação assume para escrever no bucket
resource "aws_iam_role" "pipeline_role" {
  name = "${var.bucket_name}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipeline_s3_access" {
  name = "acesso-s3-data-lake"
  role = aws_iam_role.pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      }
    ]
  })
}
