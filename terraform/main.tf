provider "aws" {
  region = var.aws_region
}

# Criação do registro ECR
resource "aws_ecr_repository" "my_ecr_repository" {
  name = var.ecr_repository_name
}

# Criação do cluster ECS
resource "aws_ecs_cluster" "my_cluster" {
  name = var.ecs_cluster_name
}

# Definição da função de execução ECS
resource "aws_iam_role" "ecs_task_execution_role" {
  name = "ecs-task-execution-role"
  
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "ecs-tasks.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

# Anexar uma política para acessar o ECR ao papel de execução da tarefa ECS
resource "aws_iam_policy_attachment" "ecs_task_execution_policy_attachment" {
  name       = "ecs-task-execution-policy-attachment"
  roles      = [aws_iam_role.ecs_task_execution_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Definição da tarefa ECS
resource "aws_ecs_task_definition" "my_task_definition" {
  family                   = var.ecs_task_family
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]

  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn

  cpu                      = 256
  memory                   = 512

  container_definitions = jsonencode([
    {
      "name"      : "jwt-container",
      "image"     : "${aws_ecr_repository.my_ecr_repository.repository_url}:latest",
      "essential" : true,
      "portMappings" : [
        {
          "containerPort" : 80,
          "hostPort"      : 80,
          "protocol"      : "tcp"
        }
      ]
    }
  ])
}


# Definição do serviço ECS
resource "aws_ecs_service" "my_service" {
  name            = var.ecs_service_name
  cluster         = aws_ecs_cluster.my_cluster.id
  task_definition = aws_ecs_task_definition.my_task_definition.arn
  launch_type     = "FARGATE"

  desired_count = 1

  network_configuration {
    subnets          = var.subnets
    assign_public_ip = true
  }
}
