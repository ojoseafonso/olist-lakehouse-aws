terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
  
  # S3 backend configuration

  backend "s3" {
    bucket         = "olist-tfstate-<account_id>" # Replace with your bucket name
    key            = "olist-lakehouse-aws/terraform/terraform.tfstate" # Object path within the bucket
    region         = "us-east-1" # Replace with your region
    encrypt        = true
    dynamodb_table = "terraform_state_lock" # Replace with your DynamoDB table name
  }
}

# Provider configuration (credentials handled via environment variables or CLI config)
provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  bucket_bronze = "${var.project_name}-bronze-${local.account_id}"
  bucket_silver = "${var.project_name}-silver-${local.account_id}"
  bucket_gold   = "${var.project_name}-gold-${local.account_id}"
  key_name      = "${var.project_name}-key"
  public_key_path = "~/.ssh/olist-lakehouse.pub"
}

resource "aws_key_pair" "key_pair_olist" {
  key_name   = local.key_name
  public_key = file(local.public_key_path)
}

resource "aws_vpc_security_group_ingress_rule" "allow_tls_ipv4" {
  security_group_id = aws_security_group.allow_tls.id
  cidr_ipv4         = aws_vpc.main.cidr_block
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.allow_tls.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}