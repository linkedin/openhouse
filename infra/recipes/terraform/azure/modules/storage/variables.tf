variable "storage_account_name" {
    type = string
    description = "The name of the Azure storage account. Must be unique."
}

variable "resource_group_name" {
    type = string
    description = "The name of the Azure resource group for the storage services."
}

variable "location" {
    type = string
    description = "The location of the Azure storage account."
}