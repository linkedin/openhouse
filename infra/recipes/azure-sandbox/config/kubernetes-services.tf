resource "azurerm_kubernetes_cluster" "sandbox" {
    name = "sandbox-k8s"
    resource_group_name = azurerm_resource_group.sandbox.name
    location = azurerm_resource_group.sandbox.location
    dns_prefix = "sandboxk8s"

    default_node_pool {
      name = "default"
      node_count = var.node_count
      vm_size = "Standard_D2_v2"
    }

    identity {
      type = "SystemAssigned"
    }

}