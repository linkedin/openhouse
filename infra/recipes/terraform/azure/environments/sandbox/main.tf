resource "azurerm_resource_group" "sandbox" {
    name = var.resource_group_name
    location = var.resource_group_location
}

module "vm" {
    source = "../../modules/vm"
    virtual_network_name = "sandbox-network"
    resource_group_name = var.resource_group_name
    subnet_name = "sandbox-subnet-vm"
    network_interface_name = "sandbox-nic"
    vm_name = "sandbox-vm"
    vm_username = "azureadmin"
    vm_password = "Pa33word"
}

module "mysql" {
    source = "../../modules/mysql"
    subnet_name = "sandbox-subnet-db"
    resource_group_name = azurerm_resource_group.sandbox.name
    server_name = "sandbox-mysql-server"
    db_admin_login = "azureadmin"
    db_admin_password = "Pa33word"
    db_name = "sandbox-db"
    virtual_network_name = module.vm.virtual_network_name
}

module "k8s" {
  source = "../../modules/k8s"
  k8s_cluster_name = "sandbox-k8s"
  resource_group_name = azurerm_resource_group.sandbox.name
  node_count = 1
  vm_size = module.vm.vm_size
}

module "storage" {
    source = "../../modules/storage"
    storage_account_name = "sandboxstorage729387" // added random string of numbers to make it unique
    resource_group_name = azurerm_resource_group.sandbox.name
}