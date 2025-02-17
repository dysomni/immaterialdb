# variables.tf

variable "table_name" {
  description = "The name of the DynamoDB table"
  type        = string
}


variable "tags" {
  description = "A map of tags to assign to the resource"
  type        = map(string)
  default     = {}
}

variable "delete_protection" {
  description = "Whether to enable delete protection on the DynamoDB table"
  type        = bool
  default     = false
}
