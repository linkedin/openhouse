output "virtual_network_name" {
    value = azurerm_virtual_network.name
}

output "vm_size" {
    value = azurerm_virtual_machine.vm.vm_size
}