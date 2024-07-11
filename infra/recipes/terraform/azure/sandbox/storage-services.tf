resource "azurerm_storage_account" "sandbox" {
    name = "sandboxstorageopenhouse"
    resource_group_name = azurerm_resource_group.sandbox.name
    location = azurerm_resource_group.sandbox.location
    account_tier = "Standard"
    account_replication_type = "LRS"
}