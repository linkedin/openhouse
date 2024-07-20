resource "helm_release" "tables" {
  chart = "tables"
  name = "openhouse-tables-service"
  repository = "../../../../k8s/helm"

  values = ["${file("../../../../k8s/environments/azure/sandbox/tables/values.yaml")}"]

  set {
    name = "storageProperties.containerName"
    value = var.container_name
  }

  set {
    name = "storageProperties.storageAccountName"
    value = var.storage_account_name
  }

  set {
    name = "storageProperties.storageAccountKey"
    value = var.storage_account_key
  }

  set_sensitive {
    name = "housetables.database.url"
    value = "mysql://${var.db_username}:${var.db_password}@${var.server_name}.mysql.database.azure.com:8080/${var.db_name}"
  }
}

resource "helm_release" "housetables" {
  chart = "housetables"
  name = "openhouse-housetables-service"
  repository = "../../../../k8s/helm"

  values = ["${file("../../../../k8s/environments/azure/sandbox/housetables/values.yaml")}"]

  # set {
  #   name = "htsService.mysql.secrets.HTS_DB_USER"
  #   value = var.db_username
  # }

  # set_sensitive {
  #   name = "htsService.mysql.secrets.HTS_DB_PASSWORD"
  #   value = var.db_password
  # }

  set_sensitive {
    name = "housetables.database.url"
    value = "mysql://${var.db_username}:${var.db_password}@${var.server_name}.mysql.database.azure.com:8080/${var.db_name}"
  }
}

# for MySQL auth
resource "kubernetes_secret" "openhouse_housetables_service_secrets" {
  metadata {
    name = "openhouse-housetables-service-secrets"
    namespace = "default"
  }

  data = {
    MYSQL_PASSWORD = var.db_password
    MYSQL_USER     = var.db_username
  }

  type = "Opaque"
}