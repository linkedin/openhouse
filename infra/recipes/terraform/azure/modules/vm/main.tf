// define the virtual network
resource "azurerm_virtual_network" "virtual-network" {
  name                = var.virtual_network_name
  address_space       = var.address_space
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

// define the subnet, the specific area in the virtual network
resource "azurerm_subnet" "subnet" {
  name = var.subnet_name
  resource_group_name =  var.resource_group_name
  virtual_network_name = azurerm_virtual_network.virtual-network.name
  address_prefixes = var.address_prefixes

  delegation {
    name = "delegation"

    service_delegation {
      name = "Microsoft.DBforMySQL/flexibleServers"
      actions = ["Microsoft.Network/networkinterfaces/*"]
    }
  }
}