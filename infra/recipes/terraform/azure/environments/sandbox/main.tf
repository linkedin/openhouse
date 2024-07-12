resource "azurerm_resource_group" "sandbox" {
    name = var.resource_group_name
    location = var.resource_group_location
}

module "vm" {
    source = "../../modules/vm"
    virtual_network_name = "sandbox-network"
    location = var.resource_group_location
    resource_group_name = var.resource_group_name
    subnet_name = "sandbox-subnet"
    network_interface_name = "sandbox-nic"
    vm_name = "sandbox-vm"
    vm_username = "azureadmin"
    vm_password = "Pa33word"
}

module "mysql" {
    source = "../../modules/mysql"
    subnet_name = "sandbox-subnet"
    resource_group_name = azurerm_resource_group.sandbox.name
    virtual_network_name = module.vm.virtual_network_name
    server_name = "sandbox-mysql-server"
    location = azurerm_resource_group.sandbox.location
    db_admin_login = "azureadmin"
    db_admin_password = "Pa33word"
    db_name = "sandbox-db"
}

module "k8s" {
  source = "../../modules/k8s"
  k8s_cluster_name = "sandbox-k8s"
  resource_group_name = azurerm_resource_group.sandbox.name
  location = azurerm_resource_group.sandbox.location
  node_count = 1
  vm_size = module.vm.vm_size
}