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

  tags {
    Name        = "sgCacheCluster"
    Project     = var.project
    Environment = var.environment
  }
}

#
# ElastiCache resources
#
resource "aws_elasticache_cluster" "memcached" {
  lifecycle {
    create_before_destroy = true
  }

  cluster_id             = "${format("%.16s-%.4s", lowevar.cache_identifier), mdvar.instance_type))}"
  engine                 = "memcached"
  engine_version         = var.engine_version
  node_type              = var.instance_type
  num_cache_nodes        = var.desired_clusters
  az_mode                = "single-az"
  parameter_group_name   = var.parameter_group
  subnet_group_name      = var.subnet_group
  security_group_ids     = ["${aws_security_group.memcached.id}"]
  maintenance_window     = var.maintenance_window
  notification_topic_arn = "${aws_sns_topic.alarm_actions_topic.arn}"
  port                   = "11211"

  tags {
    Name        = "CacheCluster"
    Project     = var.project
    Environment = var.environment
  }
}
