variable "node_count" {
    type = number
    description = "The number of nodes in the Azure k8s cluster."
}

variable "vm_size" {
    type = string
    description = "The size of the virtual machine for the Azure k8s cluster."
}