/**
Defines helm release for openhouse services. Installs tables and housetables service using the corresponding helm charts. Tables and housetables service images are referred from Azure container registry (ACR).
*/
locals {
  k8s_path = "../../../../k8s"
}

resource "helm_release" "tables" {
  chart      = "tables"
  name       = var.tables_release_name
  repository = "${local.k8s_path}/helm"

  values = ["${file("${local.k8s_path}/environments/azure/sandbox/tables/values.yaml")}"]

  set_sensitive {
    name  = "housetables.database.url"
    value = "mysql://${var.db_username}:${var.db_password}@${var.server_name}.mysql.database.azure.com:8080/${var.db_name}"
  }

  set {
    name  = "tablesService.image.repository"
    value = "${var.container_registry_name}.azurecr.io/${var.tables_release_name}"
  }

  set {
    name  = "storageProperties.containerName"
    value = var.container_name
  }

  set {
    name  = "storageProperties.storageAccountName"
    value = var.storage_account_name
  }

  set_sensitive {
    name  = "storageProperties.storageAccountKey"
    value = var.storage_account_key
  }
}

resource "helm_release" "housetables" {
  chart      = "housetables"
  name       = var.housetables_release_name
  repository = "${local.k8s_path}/helm"

  values = ["${file("${local.k8s_path}/environments/azure/sandbox/housetables/values.yaml")}"]

  set_sensitive {
    name  = "housetables.database.url"
    value = "mysql://${var.db_username}:${var.db_password}@${var.server_name}.mysql.database.azure.com:8080/${var.db_name}"
  }

  set {
    name  = "htsService.image.repository"
    value = "${var.container_registry_name}.azurecr.io/${var.housetables_release_name}"
  }
}

# for MySQL auth
resource "kubernetes_secret" "openhouse_housetables_service_secrets" {
  metadata {
    name      = "openhouse-housetables-service-secrets"
    namespace = "default"
  }

  data = {
    MYSQL_PASSWORD = var.db_password
    MYSQL_USER     = var.db_username
  }

  type = "Opaque"
}
