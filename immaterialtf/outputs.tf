# outputs.tf

output "table_name" {
  description = "The name of the DynamoDB table"
  value       = aws_dynamodb_table.table.name
}

output "table_arn" {
  description = "The ARN of the DynamoDB table"
  value       = aws_dynamodb_table.table.arn
}

output "iam_policy_arn" {
  description = "The ARN of the IAM Policy for the DynamoDB table"
  value       = aws_iam_policy.dynamodb_policy.arn
}
