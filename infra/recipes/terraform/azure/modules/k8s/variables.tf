variable "min_node_count" {
    type = number
    description = "The minimum number of nodes in the Azure k8s cluster."
}

variable "max_node_count" {
    type = number
    description = "The maximum number of nodes in the Azure k8s cluster."
}

variable "vm_size" {
    type = string
    description = "The size of the virtual machine for the Azure k8s cluster."
}

variable "acr_id" {
    type = string
    description = "The id of the Azure Container Registry that we attach to AKS"
}