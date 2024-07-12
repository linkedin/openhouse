resource "azurerm_kubernetes_cluster" "aks_cluster" {
    name = var.k8s_cluster_name
    resource_group_name = var.resource_group_name
    location = var.location
    dns_prefix = "sandboxk8s"

    default_node_pool {
      name = "default"
      node_count = var.node_count
      vm_size = var.vm_size
    }

    identity {
      type = "SystemAssigned"
    }
}