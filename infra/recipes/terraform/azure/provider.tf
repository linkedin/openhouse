provider "azurerm" {
    features {
        resource_group {
           prevent_deletion_if_contains_resources = false
        }
    }
}

provider "kubernetes" {    
  host = data.azurerm_kubernetes_cluster.default.kube_config.0.host
  client_certificate = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.client_certificate)
  client_key = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.cluster_ca_certificate)
}

data "azurerm_kubernetes_cluster" "default" {
    depends_on = [ module.k8s ]
    resource_group_name = var.resource_group_name
    name = var.k8s_cluster_name
}

# defines helm provider based on instantiated aks cluster
provider "helm" {
  kubernetes {
    host = data.azurerm_kubernetes_cluster.default.kube_config.0.host
    client_certificate = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.client_certificate)
    client_key = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.client_key)
    cluster_ca_certificate = base64decode(data.azurerm_kubernetes_cluster.default.kube_config.0.cluster_ca_certificate)
  }
}