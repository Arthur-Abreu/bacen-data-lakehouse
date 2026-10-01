output "bucket_name" {
  description = "Nome do bucket S3 criado"
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_arn" {
  description = "ARN do bucket S3 criado"
  value       = aws_s3_bucket.data_lake.arn
}

output "pipeline_role_arn" {
  description = "ARN da role IAM usada pelo pipeline para acessar o bucket"
  value       = aws_iam_role.pipeline_role.arn
}
