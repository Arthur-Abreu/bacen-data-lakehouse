variable "aws_region" {
  description = "Região AWS onde os recursos serão criados"
  type        = string
  default     = "us-east-2"
}

variable "bucket_name" {
  description = "Nome do bucket S3 (precisa ser globalmente único na AWS)"
  type        = string
  # Troque pelo nome do seu bucket antes de rodar, ex: "arthur-bcb-data-lake"
}

variable "ambiente" {
  description = "Ambiente do recurso (dev, prod, etc.)"
  type        = string
  default     = "dev"
}
