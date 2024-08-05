output "virtual_network_name" {
  value = azurerm_virtual_network.virtual-network.name
}

output "subnet_id" {
  value = azurerm_subnet.subnet.id
}

output "dns_zone_id" {
  value = azurerm_private_dns_zone.default.id
}
