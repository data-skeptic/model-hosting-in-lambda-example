resource "aws_sns_topic" "alarm_actions_topic" {
  name = "alarm-actions-topic"
  delivery_policy = <<EOF
{
  "http": {
    "defaultHealthyRetryPolicy": {
      "minDelayTarget": 20,
      "maxDelayTarget": 20,
      "numRetries": 3,
      "numMaxDelayRetries": 0,
      "numNoDelayRetries": 0,
      "numMinDelayRetries": 0,
      "backoffFunction": "linear"
    },
    "disableSubscriptionOverrides": false,
    "defaultThrottlePolicy": {
      "maxReceivesPerSecond": 1
    }
  }
}
EOF
}


#
# Security group resources
#
resource "aws_security_group" "memcached" {
  vpc_id = var.vpc_id
}

#
# ElastiCache resources
#
resource "aws_elasticache_cluster" "memcached" {
  cluster_id           = "feaas-cluster"
  engine               = "memcached"
  node_type            = var.instance_type
  num_cache_nodes      = 1
  parameter_group_name = "default.memcached1.4"
  port                 = 11211
}
