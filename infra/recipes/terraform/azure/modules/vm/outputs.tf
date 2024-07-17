output "virtual_network_name" {
    value = azurerm_virtual_network.virtual-network.name
}

output "subnet_id" {
    value = azurerm_subnet.subnet.id
}