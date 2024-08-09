/**
Defines the Azure container registry (ACR) to hold image builds.
*/

resource "azurerm_container_registry" "default" {
  name                = var.unique_registry_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  sku                 = "Premium"
  admin_enabled       = true
}
