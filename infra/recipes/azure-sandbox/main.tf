terraform {
  required_providers {
    azurerm = {
        source = "hashicorp/azure"
        version = "=3.0.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "sandbox" {
    name = "sandbox-resources"
    location = "West US 2"
}

