variable "aws_region" {
  description = "Região da AWS"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "Tipo de instância EC2 (Free Tier: t3.micro)"
  type        = string
  default     = "t3.micro"
}

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "olist-lakehouse"
}

variable "iam_role_name" {
  description = "Nome da role IAM"
  type        = string
  default     = "olist-ec2-role"
}

variable "allowed_ssh_cidr" {
  description = "CIDR autorizado para SSH (seu IP: curl ifconfig.me/32)"
  type        = string
}