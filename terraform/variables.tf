# variables.tf

variable "aws_region" {
  description = "AWS Region"
  default     = "us-east-1"
}

variable "ecs_cluster_name" {
  description = "ECS Cluster Name"
  default     = "jwt-cluster"
}

variable "ecr_repository_name" {
  description = "ECR Repository Name"
  default     = "jwt-repository"
}

variable "ecs_service_name" {
  description = "ECS Service Name"
  default     = "jwt-service"
}

variable "ecs_task_family" {
  description = "ECS Task Family"
  default     = "nome_da_sua_task_family"
}

variable "subnets" {
  description = "List of subnet IDs for ECS Service"
  type        = list(string)
  default     = ["subnet-06a7ec692f0180269", "subnet-0bb4219e2fb41fbd8"]
}