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

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "main" {
  name        = "${var.project_name}-sg"
  description = "Security group do projeto ${var.project_name}"
  vpc_id      = data.aws_vpc.default.id 

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
    description = "SSH"
  }

    ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
    description = "Airflow Webserver"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

  tags = {
    Name    = "${var.project_name}-sg"
    Project = var.project_name
  }
}

#Define quem é o dono da role para executar algo
resource "aws_iam_role" "main" {
  name               = "${var.project_name}-role"

#Define a role, no caso assumir a função de executor por parte do EC2
  inline_policy {
    name = "my_inline_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = ["sts:AssumeRole"]
          Principal = { "Service": "ec2.amazonaws.com" }
          Effect   = "Allow"
        }
      ]
    })
  }

    tags = {
    Name    = "${var.project_name}-role"
    Project = var.project_name
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name   = "${var.project_name}-s3-policy"
  role = aws_iam_role.main.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ]
      Resource = [
        aws_s3_bucket.bronze.arn,
        "${aws_s3_bucket.bronze.arn}/*",
        aws_s3_bucket.silver.arn,
        "${aws_s3_bucket.silver.arn}/*",
        aws_s3_bucket.gold.arn,
        "${aws_s3_bucket.gold.arn}/*",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "main" {
  name = "${var.project_name}-role-profile"
  role = aws_iam_role.main.id

resource "aws_s3_bucket" "bronze" {
  bucket = local.bucket_bronze
  tags = {
   Name        = local.bucket_bronze
   Project     = var.project_name
   Environment = "dev"
   Layer       = "bronze"
  }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "silver" {
  bucket = local.bucket_silver
  tags = {
   Name        = local.bucket_silver
   Project     = var.project_name
   Environment = "dev"
   Layer       = "silver"
  }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "gold" {
  bucket = local.bucket_gold
  tags = {
   Name        = local.bucket_gold
   Project     = var.project_name
   Environment = "dev"
   Layer       = "gold"
  }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_instance" "main" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type
  key_name      = aws_key_pair.main.key_name
  vpc_security_group_ids = [aws_security_group.main.id]
  iam_instance_profile =   aws_iam_instance_profile.main.id
  user_data = file("user_data.sh")

  tags = {
  Name    = "${var.project_name}-ec2"
  Project = var.project_name
}