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

variable "container_registry_name" {
  type        = string
  description = "The name of the container registry where the build images are stored."
}

variable "tables_release_name" {
  type        = string
  default     = "openhouse-tables-service"
  description = "The name of the tables service Helm release."
}

variable "housetables_release_name" {
  type        = string
  default     = "openhouse-housetables-service"
  description = "The name of the tables service Helm release."
}
