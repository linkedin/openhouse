/**
This container environment is needed to provision the Azure Container Registry (ACR)
before any other provider is created. For example, the Docker resource needs data from
the ACR resource, so we need a separate environment to ensure ACR is provisioned before
we instantiate the Docker resource.
*/

resource "azurerm_resource_group" "openhouse_sandbox" {
  name     = var.resource_group_name
  location = var.resource_group_location
}

module "container" {
  source = "../../modules/container"
}
