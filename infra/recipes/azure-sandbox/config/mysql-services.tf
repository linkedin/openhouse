resource "azurerm_mysql_server" "sandbox" {
  name = "sandbox-sqlserver"
  resource_group_name = azurerm_resource_group.sandbox.name
  location = azurerm_resource_group.sandbox.location
  version = "8.0"
  administrator_login = var.username
  administrator_login_password = var.password // needs to comply with Azure password policy
  ssl_enforcement_enabled = true
  sku_name   = "B_Gen5_2"
}

resource "azurerm_mysql_database" "sandbox" {
  name = "sandbox-db"
  resource_group_name = azurerm_resource_group.sandbox.name
  server_name = azurerm_mysql_server.sandbox.name
  collation      = "SQL_Latin1_General_CP1_CI_AS" // for sorting
  charset = "utf8"
  # license_type   = "LicenseIncluded" // need a license
  # max_size_gb    = 4
  # read_scale     = true // readonly connections may be routed to secondary database
  # sku_name       = "S0" // stock keeping unit
  # zone_redundant = false // not needed for now
  # enclave_type   = "VBS" // uses hardware virtualization to create isolated environments to protect sensitive processes/data

  # tags = {} // ways to add metadata if needed

  # prevent the possibility of accidental data loss
  lifecycle {
    prevent_destroy = true
  }
}