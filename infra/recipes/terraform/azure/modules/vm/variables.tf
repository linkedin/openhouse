variable "virtual_network_name" {
    type = string
    description = "The name of the virtual network."
}

variable "subnet_name" {
    type = string
    description = "The name of the Azure subnet."
}

variable "address_space" {
    type = list(string)
    description = "The address space for the virtual network."
    default = ["10.0.0.0/16"]
}

variable "address_prefixes" {
    type = list(string)
    description = "The address prefixes for the virtual machine subnet."
    default = ["10.0.1.0/24"]
}

