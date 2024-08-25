# main.tf

resource "aws_dynamodb_table" "table" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "entity_id"
    type = "S"
  }

  attribute {
    name = "entity_name"
    type = "S"
  }

  attribute {
    name = "base_node_id"
    type = "S"
  }

  global_secondary_index {
    name            = "ids_only"
    hash_key        = "entity_id"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "model_scan"
    hash_key        = "entity_name"
    range_key       = "base_node_id"
    projection_type = "ALL"
  }

  tags = var.tags
}


resource "aws_iam_policy" "dynamodb_policy" {
  name = "${var.table_name}_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ],
        Effect = "Allow",
        Resource = [
          "${aws_dynamodb_table.table.arn}",
          "${aws_dynamodb_table.table.arn}/*"
        ]
      }
    ]
  })
}
