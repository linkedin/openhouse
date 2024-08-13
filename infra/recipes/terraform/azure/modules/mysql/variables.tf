variable "subnet_id" {
  type        = string
  description = "The id of the MySQL subnet."
}

variable "server_name" {
  type        = string
  description = "The name for the MySQL server."
}

variable "db_admin_login" {
  type        = string
  description = "The username for the MySQL server admin."
}

variable "db_admin_password" {
  type        = string
  description = "The password for the MySQL server admin."
  sensitive   = true
}

variable "db_name" {
  type        = string
  description = "The name for the MySQL database."
}

variable "server_sku" {
  type        = string
  description = "The SKU for the MySQL flexible server."
  default     = "GP_Standard_D2ds_v4"
}

variable "dns_zone_id" {
  type        = string
  description = "The id of the private DNS zone within Azure DNS."
}
