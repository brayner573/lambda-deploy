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

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::upeu-producer-x-bucket-1"
}

resource "aws_s3_bucket_notification" "lambda_trigger" {
  bucket = "upeu-producer-x-bucket-1"

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "uploads/"
    filter_suffix       = ".xlsx"
  }

  depends_on = [aws_lambda_permission.allow_s3]
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
#prueba