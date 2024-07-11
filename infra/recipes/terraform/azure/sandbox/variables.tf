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

variable "node_count" {
  type        = number
  description = "The initial quantity of nodes for the node pool."
  default     = 1
}