/**
Defines k8s module resources and configuration. These includes AKS cluster configuration, node pool, role assignment etc.
*/
resource "azurerm_kubernetes_cluster" "aks_cluster" {
  name                = var.k8s_cluster_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  dns_prefix          = "sandboxk8s"

  default_node_pool {
    name                = "default"
    min_count           = var.min_node_count
    max_count           = var.max_node_count
    enable_auto_scaling = true
    vm_size             = var.vm_size
  }

  identity {
    type = "SystemAssigned"
  }
}

# add the role to the identity the kubernetes cluster was assigned
resource "azurerm_role_assignment" "aks_to_acr" {
  scope                = var.acr_id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.aks_cluster.kubelet_identity[0].object_id
}
