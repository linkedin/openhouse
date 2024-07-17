provider "azurerm" {
    features {
        resource_group {
           prevent_deletion_if_contains_resources = false
        }
    }
}

provider "kubernetes" {
  host                   = module.k8s.host
  client_certificate     = var.k8s_client_certificate
  client_key             = var.k8s_client_key
  cluster_ca_certificate = var.k8s_cluster_ca_certificate
}