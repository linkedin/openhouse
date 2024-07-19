resource "azurerm_resource_group" "openhouse_sandbox" {
    name = var.resource_group_name
    location = var.resource_group_location
}

module "container" {
    source = "../../modules/container"
}