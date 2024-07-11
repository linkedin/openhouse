terraform {
  required_providers {
    azurerm = {
        source = "hashicorp/azurerm"
        version = "=3.109.0"
    }
  }
}

resource "azurerm_resource_group" "sandbox" {
    name = var.resource_group_name
    location = var.resource_group_location
}