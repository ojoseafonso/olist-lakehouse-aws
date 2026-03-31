output "public_dns" {
  description = "IP público ou DNS do EC2"
  value       = aws_instance.main.public_dns
}

output "aws_s3_bucket_bronze" {
  description = "Nome do bucket bronze"
  value       = aws_s3_bucket.bronze.bucket
}

output "aws_s3_bucket_silver" {
  description = "Nome do bucket silver"
  value       = aws_s3_bucket.silver.bucket
}

output "aws_s3_bucket_gold" {
  description = "Nome do bucket gold"
  value       = aws_s3_bucket.gold.bucket
}

output "aws_iam_profile" {
  description = "Nome do iam profile"
  value       = aws_iam_instance_profile.main.name 
}