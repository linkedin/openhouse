## Instructions to configure and run the Azure Sandbox

Install the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/), and ensure that you are logged in with your Azure account by running `az login`.

Then, run `terraform init` to initialize the terraform and `terraform plan` to confirm the configuration that will be made. Once satisfied with the planned changes, run `terraform apply` to create the configuration in your Azure account.