## Instructions to configure and run the Azure Sandbox

Create a Microsoft account and sign in to your Azure Portal. Ensure that you have a subscription created; if this is your first time using Azure Portal, you can use the Free Trial Subscription.

Install the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/), and ensure that you are logged in with your Azure account by running `az login`. Right now, the simple `az login` with username/password is used. However, in the future it may be fruitful to utilize [logging in with a service principal](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-service-principal): `az login --service-principal -u <app-id> -p <password-or-cert> --tenant <tenant>`. This option also gives more control over subscription types.

Then, run `terraform init` to initialize the terraform and `terraform plan` to confirm the configuration that will be made. Once satisfied with the planned changes, run `terraform apply` to create the configuration in your Azure account.