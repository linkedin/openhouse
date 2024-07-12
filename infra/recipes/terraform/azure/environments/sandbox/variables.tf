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

variable "storage_account_name" {
  type = string
  description = "Name of the storage account for the backend."
}