variable "container_name" {
  type        = string
  description = "The name of the container for ADLS storage."
}

variable "storage_account_name" {
  type        = string
  description = "The name of the storage account for ADLS storage."
}

variable "storage_account_key" {
  type        = string
  description = "The key for the storage account for ADLS storage."
  sensitive   = true
}

variable "db_username" {
  type        = string
  description = "The username for the MySQL database."
}

variable "db_password" {
  type        = string
  description = "The password for the MySQL database."
  sensitive   = true
}

variable "server_name" {
  type        = string
  description = "The name of the MySQL server."
}

variable "db_name" {
  type        = string
  description = "The name of the MySQL database."
}
