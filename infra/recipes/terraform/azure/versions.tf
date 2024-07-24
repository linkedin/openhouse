terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "< 3.90.0"
    }

    docker = {
      source  = "kreuzwerker/docker"
      version = "3.0.2"
    }
  }
}
