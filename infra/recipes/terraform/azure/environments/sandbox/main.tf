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
    subnet_id = module.vm.subnet_id
    resource_group_name = azurerm_resource_group.openhouse_sandbox.name
    server_name = "openhouse-sandbox-mysql-server"
    db_admin_login = "azureadmin"
    db_admin_password = "Pa33word"
    db_name = "openhouse-sandbox-db"
}

module "k8s" {
  source = "../../modules/k8s"
  k8s_cluster_name = "openhouse-sandbox-k8s"
  resource_group_name = azurerm_resource_group.openhouse_sandbox.name
  node_count = 1
  vm_size = "Standard_D2s_v3"
}

module "storage" {
    source = "../../modules/storage"
    storage_account_name = "openhousestorage${random_string.storage_name.result}" // added random string of numbers to make it unique
    resource_group_name = azurerm_resource_group.openhouse_sandbox.name
}

module "helm_release" {
    source = "../../modules/helm_release"
    depends_on = [ module.k8s ] // so k8s cluster is instantiated before helm deployment
}