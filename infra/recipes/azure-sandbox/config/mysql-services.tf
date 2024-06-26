resource "azurerm_subnet" "mysql_subnet" {
  name = "mysql-subnet"
  resource_group_name =  azurerm_resource_group.sandbox.name
  virtual_network_name = azurerm_virtual_network.sandbox.name
  address_prefixes = ["10.0.0.0/24"]

  delegation {
    name = "delegation"

    service_delegation {
      name = "Microsoft.DBforMySQL/flexibleServers"
      actions = ["Microsoft.Network/networkinterfaces/*"]
    }
  }
}

resource "azurerm_mysql_flexible_server" "sandbox" {
  name = "sandbox-sqlserver"
  resource_group_name = azurerm_resource_group.sandbox.name
  location = azurerm_resource_group.sandbox.location
  administrator_login = var.username
  administrator_password = var.password // needs to comply with Azure password policy
  delegated_subnet_id = azurerm_subnet.mysql_subnet.id
  sku_name = "GP_Standard_D2ds_v4"
}

resource "azurerm_mysql_flexible_database" "sandbox" {
  name = "sandbox-db"
  resource_group_name = azurerm_resource_group.sandbox.name
  server_name = azurerm_mysql_flexible_server.sandbox.name
  collation = "utf8_unicode_ci" // for sorting
  charset = "utf8"
}
