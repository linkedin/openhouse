provider "azurerm" {
    features {}
}

provider "kubernetes" {
  host                   = module.k8s.host
  client_certificate     = base64decode(module.k8s.client_certificate)
  client_key             = base64decode(module.k8s.client_key)
  cluster_ca_certificate = base64decode(module.k8s.cluster_ca_certificate)
}