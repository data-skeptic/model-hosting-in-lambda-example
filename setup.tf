variable "aws_region" {
  type    = string
}

variable "aws_accesskey" {
  type    = string
}

variable "aws_secretkey" {
  type    = string
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_accesskey
  secret_key = var.aws_secretkey
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "cluster-redis"
  engine               = "redis"
  node_type            = "cache.m4.large"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis3.2"
  engine_version       = "3.2.10"
  port                 = 6379
}

#output "ec2instance" {
#  value = aws_elasticache_cluster.public_ip
#}

#terraform.tfvars