/**
Defines the virtual networks and subnets.
*/

resource "azurerm_virtual_network" "virtual-network" {
  name                = var.virtual_network_name
  address_space       = var.address_space
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

resource "azurerm_subnet" "subnet" {
  name                 = var.subnet_name
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.virtual-network.name
  address_prefixes     = var.address_prefixes
  service_endpoints    = ["Microsoft.Storage"]

  delegation {
    name = "delegation"

    service_delegation {
      name    = "Microsoft.DBforMySQL/flexibleServers"
      actions = ["Microsoft.Network/networkinterfaces/*", "Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

// Enables you to manage Private DNS zones within Azure DNS
resource "azurerm_private_dns_zone" "default" {
  name                = var.dns_zone_name
  resource_group_name = var.resource_group_name
}

// Enables you to manage Private DNS zone Virtual Network Links
resource "azurerm_private_dns_zone_virtual_network_link" "default" {
  name                  = var.dns_link_name
  private_dns_zone_name = azurerm_private_dns_zone.default.name
  resource_group_name   = var.resource_group_name
  virtual_network_id    = azurerm_virtual_network.virtual-network.id

  depends_on = [azurerm_subnet.subnet]
}
