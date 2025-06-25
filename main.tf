terraform {
  backend "s3" {
    bucket = "upeu-terraform-state"
    key    = "lambda/main.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}

# Referencia al estado remoto del módulo S3
data "terraform_remote_state" "s3" {
  backend = "s3"
  config = {
    bucket = "upeu-terraform-state"
    key    = "s3/main.tfstate"
    region = "us-east-1"
  }
}

# Reutiliza el rol IAM existente llamado "lambda_exec_role"
data "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"
}

# Lambda function 
resource "aws_lambda_function" "lambda" {
  function_name    = var.lambda_name
  filename         = "lambda_function_payload.zip"
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  role             = data.aws_iam_role.lambda_exec.arn
  source_code_hash = filebase64sha256("lambda_function_payload.zip")

  environment {
    variables = {
      INPUT_BUCKET  = data.terraform_remote_state.s3.outputs.x_bucket_name
      OUTPUT_BUCKET = data.terraform_remote_state.s3.outputs.x_output_bucket_name
    }
  }
}
