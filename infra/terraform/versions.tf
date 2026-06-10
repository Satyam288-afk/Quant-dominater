terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state is recommended for any shared environment. Configure and
  # uncomment, then `terraform init -migrate-state`.
  # backend "s3" {
  #   bucket         = "iicpc-tfstate"
  #   key            = "benchmark-platform/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "iicpc-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "iicpc-benchmark-platform"
      ManagedBy = "terraform"
      Env       = var.environment
    }
  }
}
