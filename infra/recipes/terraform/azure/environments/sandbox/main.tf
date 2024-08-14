/**
This environment is used to provision the configuration and resources needed
for the Azure sandbox to host Openhouse services. The following resources are
created/used:

- azurerm_resource_group: The Azure resource group that all other resources use.
- locals: Local variables that specify configuration settings.
- module.vm: The virtual network and subnet where other resources are located.
- module.mysql: The MySQL database for HTS.
- module.storage: The Azure storage accounts and containers.
- module.k8s: The AKS clusters.
- module.helm_release: The helm releases of the Tables and HTS Services.
- module.image: The Docker images for Tables and HTS and their connections to the Azure Container Registry.
*/

data "azurerm_resource_group" "openhouse_sandbox" {
  name = var.resource_group_name
}

data "azurerm_storage_account" "default" {
  depends_on          = [module.storage]
  resource_group_name = var.resource_group_name
  name                = local.storage_account_name
}

resource "random_string" "name" {
  length  = 6
  special = false
  lower   = true
  upper   = false
}

locals {
  db_username          = "azureadmin"
  db_password          = "Pa33word"
  db_name              = "openhouse-sandbox-db"
  db_server_name       = "openhouse-sandbox-mysql-server${random_string.name.result}"
  storage_account_name = "openhousestorage${random_string.name.result}"
  container_name       = "blobcontainer"
}

module "vm" {
  source               = "../../modules/vm"
  depends_on           = [data.azurerm_resource_group.openhouse_sandbox]
  virtual_network_name = "openhouse-sandbox-network"
  resource_group_name  = var.resource_group_name
  subnet_name          = "openhouse-sandbox-subnet"
  dns_zone_name        = "openhouse-dns-zone.mysql.database.azure.com"
  dns_link_name        = "openhouse-dns-link"
}

module "mysql" {
  source              = "../../modules/mysql"
  depends_on          = [module.vm]
  subnet_id           = module.vm.subnet_id
  resource_group_name = data.azurerm_resource_group.openhouse_sandbox.name
  server_name         = local.db_server_name
  db_admin_login      = local.db_username
  db_admin_password   = local.db_password
  db_name             = local.db_name
  server_sku          = "B_Standard_B2ms" # 2 vCPU and 8 GiB memory
  dns_zone_id         = module.vm.dns_zone_id
}

module "storage" {
  source               = "../../modules/storage"
  depends_on           = [data.azurerm_resource_group.openhouse_sandbox]
  storage_account_name = local.storage_account_name
  container_name       = local.container_name
}

module "k8s" {
  source              = "../../modules/k8s"
  depends_on          = [data.azurerm_resource_group.openhouse_sandbox]
  k8s_cluster_name    = var.k8s_cluster_name
  resource_group_name = data.azurerm_resource_group.openhouse_sandbox.name
  min_node_count      = 2
  max_node_count      = 5
  vm_size             = "Standard_B2pls_v2" # 2 vCPU and 4 GiB memory
  acr_id              = data.azurerm_container_registry.default.id
}

module "helm_release" {
  source                  = "../../modules/helm_release"
  depends_on              = [module.k8s] // so k8s cluster is instantiated before helm deployment
  db_username             = local.db_username
  db_password             = local.db_password
  db_name                 = local.db_name
  server_name             = local.db_server_name
  container_registry_name = data.azurerm_container_registry.default.name
  storage_account_name    = local.storage_account_name
  storage_account_key     = data.azurerm_storage_account.default.primary_access_key
  container_name          = local.container_name
}

module "image" {
  source                = "../../modules/image"
  registry_login_server = data.azurerm_container_registry.default.login_server
}
