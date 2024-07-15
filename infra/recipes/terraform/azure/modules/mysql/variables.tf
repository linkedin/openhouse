variable "subnet_name" {
    type = string
    description = "The name for the MySQL subnet."
}

variable "server_name" {
    type = string
    description = "The name for the MySQL server."
}

variable "db_admin_login" {
    type = string
    description = "The username for the MySQL server admin."
}

variable "db_admin_password" {
    type = string
    description = "The password for the MySQL server admin."
    sensitive = true
}

variable "db_name" {
    type = string
    description = "The name for the MySQL database."
}

variable "virtual_network_name" {
    type = string
    description = "The name of the virtual network for the MySQL subnet."
}

variable "address_prefixes" {
    type = list(string)
    description = "The address prefixes for the MySQL subnet."
    default = ["10.0.0.0/24"]
}

variable "server_sku" {
    type = string
    description = "The SKU for the MySQL flexible server."
    default = "GP_Standard_D2ds_v4"
}