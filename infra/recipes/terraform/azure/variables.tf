variable "k8s_client_certificate" {
  type = string
  description = "The client certificate for AKS."
}

variable "k8s_client_key" {
  type = string
  description = "The client key for AKS."
  sensitive = true
}

variable "k8s_cluster_ca_certificate" {
  type = string
  description = "The cluster certificate for AKS."
}

variable "resource_group_name" {
    type = string
    default = "resources"
    description = "Name of the resource group."
}

variable "resource_group_location" {
  type        = string
  default     = "westus"
  description = "Location of the resource group."
}