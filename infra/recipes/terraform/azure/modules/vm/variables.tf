variable "virtual_network_name" {
    type = string
    description = "The name of the virtual network."
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
    default = "Standard_D2s_v3" // compatible with Azure Kubernetes Cluster
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

