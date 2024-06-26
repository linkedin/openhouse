variable "resource_group_name" {
    type = string
    default = "sandbox-resources"
    description = "Name of the resource group."
}

variable "resource_group_location" {
  type        = string
  default     = "westus"
  description = "Location of the resource group."
}

variable "node_count" {
  type        = number
  description = "The initial quantity of nodes for the node pool."
  default     = 1
}

variable "username" {
  type        = string
  description = "The admin username for the virtual machine and MySQL server."
  default     = "azureadmin"
}

variable "password" {
    type = string
    description = "The admin password for the virtual machine and MySQL server."
    default = "Pa33word"
}

