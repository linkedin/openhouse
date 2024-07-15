resource "azurerm_subnet" "mysql_subnet" {
  name = var.subnet_name
  resource_group_name = var.resource_group_name
  virtual_network_name = var.virtual_network_name
  address_prefixes = var.address_prefixes

  delegation {
    name = "delegation"

    service_delegation {
      name = "Microsoft.DBforMySQL/flexibleServers"
      actions = ["Microsoft.Network/networkinterfaces/*"]
    }
  }
}

resource "azurerm_mysql_flexible_server" "mysql_server" {
  name = var.server_name
  resource_group_name = var.resource_group_name
  location = var.resource_group_location
  administrator_login = var.db_admin_login
  administrator_password = var.db_admin_password
  delegated_subnet_id = azurerm_subnet.mysql_subnet.id
  sku_name = var.server_sku
}

resource "azurerm_mysql_flexible_database" "mysql" {
  name = var.db_name
  resource_group_name = var.resource_group_name
  server_name = azurerm_mysql_flexible_server.mysql_server.name
  collation = "utf8_unicode_ci" // for sorting
  charset = "utf8"
}