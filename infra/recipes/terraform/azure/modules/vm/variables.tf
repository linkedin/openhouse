variable "virtual_network_name" {
    type = string
    description = "The name of the virtual network."
}

variable "location" {
    type = string
    description = "The location for the virtual network."
}

variable "resource_group_name" {
    type = string
    description = "The name of the resource group associated with the virtual machine resources."
}

variable "subnet_name" {
    type = string
    description = "The name of the Azure subnet."
}

variable "network_interface_name" {
    type = string
    description = "The name of the Azure network interface."
}

variable "vm_name" {
    type = string
    description = "The name of the Azure virtual machine."
}

variable "vm_size" {
    type = string
    description = "The size of the virtual machine."
    default = "Standard_DS1_v2"
}

variable "vm_username" {
    type = string
    description = "The username for the virtual machine OS profile."
}

variable "vm_password" {
    type = string
    description = "The password for the virtual machine OS profile."
    sensitive = true
}