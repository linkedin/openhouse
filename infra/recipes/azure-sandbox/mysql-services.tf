resource "azurerm_mysql_server" "sandbox" {
  name = "sandbox-sqlserver"
  resource_group_name = azurerm_resource_group.sandbox.name
  location = azurerm_resource_group.sandbox.location
  version = "12.0"
  administrator_login = "admin"
  administrator_login_password = "Pa33word" // needs to comply with Azure password policy
}

resource "azurerm_mysql_database" "sandbox" {
  name = "sandbox-db"
  server_id = azurerm_mysql_server.sandbox.id
  collation      = "SQL_Latin1_General_CP1_CI_AS" // for sorting
  license_type   = "LicenseIncluded" // need a license
  max_size_gb    = 4
  read_scale     = true // readonly connections may be routed to secondary database
  sku_name       = "S0" // stock keeping unit
  zone_redundant = false // not needed for now
  enclave_type   = "VBS" // uses hardware virtualization to create isolated environments to protect sensitive processes/data

  tags = {} // ways to add metadata if needed

  # prevent the possibility of accidental data loss
  lifecycle {
    prevent_destroy = true
  }
}