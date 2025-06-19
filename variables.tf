variable "aws_region" {
  default = "us-east-1"
}

variable "lambda_function_name" {
  default = "lambda-integrada"
}

variable "s3_bucket_name" {
  description = "Nombre del bucket S3 al que Lambda accede"
  type        = string
}

variable "ec2_endpoint" {
  description = "URL pública de la instancia EC2"
  type        = string
}
