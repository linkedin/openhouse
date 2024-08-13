output "resource_group_name" {
  value = data.azurerm_resource_group.openhouse_sandbox.name
}

output "storage_account_name" {
  value = local.storage_account_name
}

output "storage_account_key" {
  value     = data.azurerm_storage_account.default.primary_access_key
  sensitive = true
}

output "aks_cluster_name" {
  value = var.k8s_cluster_name
}
