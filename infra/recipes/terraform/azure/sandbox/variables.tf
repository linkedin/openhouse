variable "username" {
  type        = string
  description = "The admin username for multiple services."
  default     = "azureadmin"
}

variable "password" {
    type = string
    description = "The admin password for multiple services."
    default = "Pa33word"
}

variable "node_count" {
  type        = number
  description = "The initial quantity of nodes for the node pool."
  default     = 1
}

variable "vm_size" {
  type = string
  description = "The size of the virtual machine."
  default = "Standard_D2_v2"
}