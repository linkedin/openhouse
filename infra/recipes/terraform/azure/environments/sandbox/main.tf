resource "azurerm_resource_group" "openhouse_sandbox" {
    name = var.resource_group_name
    location = var.resource_group_location
}

resource "random_string" "storage_name" {
    length = 5
    special = false
}

module "vm" {
    source = "../../modules/vm"
    virtual_network_name = "openhouse-sandbox-network"
    resource_group_name = var.resource_group_name
    subnet_name = "openhouse-sandbox-subnet"
}

module "mysql" {
    source = "../../modules/mysql"
    depends_on = [ module.vm ]
    subnet_id = module.vm.subnet_id
    resource_group_name = azurerm_resource_group.openhouse_sandbox.name
    server_name = local.db_server_name
    db_admin_login = local.db_username
    db_admin_password = local.db_password
    db_name = var.db_name
}

module "k8s" {
  source = "../../modules/k8s"
  k8s_cluster_name = "openhouse-sandbox-k8s"
  resource_group_name = azurerm_resource_group.openhouse_sandbox.name
  node_count = 1
  vm_size = "Standard_D2s_v3"
}

locals {
    storage_account_name = "openhousestorage${random_string.storage_name.result}" // added random string of numbers to make it unique
    container_name = "blobcontainer"
    db_username = "azureadmin"
    db_password = "Pa33word"
    db_name = "openhouse-sandbox-db"
    db_server_name = "openhouse-sandbox-mysql-server"
}

module "storage" {
    source = "../../modules/storage"
    storage_account_name = local.storage_account_name
    resource_group_name = azurerm_resource_group.openhouse_sandbox.name
    container_name = local.container_name
}

module "helm_release" {
    source = "../../modules/helm_release"
    depends_on = [ module.k8s ] // so k8s cluster is instantiated before helm deployment
}

data "azurerm_storage_account" "default" {
    depends_on = [ module.storage ]
    resource_group_name = var.resource_group_name
    name = local.storage_account_name
}