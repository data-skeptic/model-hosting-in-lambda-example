variable "project" {
  default = "Lambda Models"
}

variable "environment" {
  default = "Unknown"
}

variable "vpc_id" {}

variable "cache_identifier" {
  default = "cache"
}

variable "subnet_group" {
}

variable "maintenance_window" {
  default = "sun:02:30-sun:03:30"
}

variable "desired_clusters" {
  default = "1"
}

variable "instance_type" {
  default = "cache.t2.micro"
}

variable "engine_version" {
  default = "1.4.33"
}

variable "alarm_cpu_threshold_percent" {
  default = "75"
}

variable "alarm_memory_threshold_bytes" {
  # 10MB
  default = "10000000"
}

