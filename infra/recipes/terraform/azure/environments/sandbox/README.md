### Initialize and Run Azure Sandbox

Ensure you have a functional Azure account with a resource group, storage account, and blob container within that storage account. Replace the variables in `config.azurerm.tfbackend` with their respective values with respect to your account. This is where the Terraform state files will be stored.

To configure the sandbox, run:

```
terraform init -backend-config="./config.azurerm.tfbackend
terraform plan
```

If you are satisfied with the plan, run `terraform apply` to apply the configuration.
