

resource "azurerm_container_registry" "default" {
  name                = var.registry_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  sku                 = "Premium"
  admin_enabled       = true
}