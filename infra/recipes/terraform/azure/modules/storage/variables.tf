variable "storage_account_name" {
  type        = string
  description = "The name of the Azure storage account. Must be unique."
}

variable "container_name" {
  type        = string
  description = "The name of the container withing the storage account."
}