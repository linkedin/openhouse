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