# Initialize and Run Azure Sandbox

## Creating an Azure Account

Create an Azure account [here](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account). If you haven't created one already, you can use the free trial to get free credits. Create a subscription using this free trial, or with another option if the free trial is over.

## Azure Login

Install the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/), and ensure that you are logged in with your Azure account. You can login in multiple ways.

### Using username/password

This is the simplest version. Run `az login` and login to your account on a web browser.

### Using Service Principal

Following the steps [here](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-service-principal), you can run `az login --service-principal -u <app-id> -p <password-or-cert> --tenant <tenant>` to gain more control over your subscription and account.

## OpenHouse  - Build

Follow the steps described in [SETUP.md](SETUP.md#build-containers) to build JAR artifacts locally for OpenHouse services using Gradle. Currently, running only tables and housetables service is supported in Azure.

## Deployment

### Before you begin

Install the `terraform` CLI by following [this](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) link.

### Planning & Applying

#### Azure Container Registry - Setup

You can use the Terraform code provided in [Azure Container Terraform Environmnet](infra/recipes/terraform/azure/environments/container) to configure the ACR.

Run the following three commands for ACR publishing

1. `terraform init` : Run it first time to initialize backend
2. `terraform plan` : Plan the Terraform deployment (optional)
3. `terraform apply` : Apply the Terraform deployment

#### Rest of Infra - Setup

As part of bringing OpenHouse in Azure, you will need to build the Docker images and setup other Azure services like AKS, MySQL, Storage. You can use the Terraform code provided in [Azure Sandbox Terraform Environment](infra/recipes/terraform/azure/environments/sandbox) to setup rest of infra that is required for successfully bringing up OpenHouse.

Run the following three commands for rest of infra setup

1. `terraform init` : Run it first time to initialize backend
2. `terraform plan` : Plan the Terraform deployment (optional)
3. `terraform apply` : Apply the Terraform deployment

## Deconfiguration

Run `terraform destroy` once you are done to tear down the services you have created.

# Access Created Services

## Run client shell

### Start local spark cluster

Navigate to `infra/recipes/docker-compose/spark-only`, and run
```
docker compose up -d
```
to build the images and start the `local.spark-master` container.

### Start spark-shell

Once the terraform services have started on AKS, from the `sandbox` directory run
```
./spark-client.sh
```
This script gets the AKS cluster info such storage account and AKS cluster using terraform. And then pulls the details of the services running on AKS such as tables-service  address and port and generates the spark-shell start command. Lastly, starts spark shell within the `local.spark-master` container. Then, run commands like those found in `SETUP.md` to make requests from the spark client.

## Access via CURL requests

You can access the pods in AKS in your local terminal by ensuring you are logged in to the Azure CLI with `az login` and then running
`az aks get-credentials --resource-group <resource-group> --name <cluster-name>`. You can then run any `kubectl` commands.

### Make requests to services

To make requests to the services, set up port forwarding by running `kubectl port-forward <pod-name> 8080:<destination-port>`. You can get the pod name by running `kubectl get pods`. Then, you can make requests to HTS and Tables service via `http://localhost:<destination-port>`. If you want to make requests to both HTS and Tables services you will need to forward to two separate ports.

# Troubleshooting

When running `terraform destroy`, you may encounter errors or timeouts. To unblock this, you can always manually delete resources from your Azure Portal. To ensure that the terraform state is synced, you can run `terraform state list` to see the current resources in the state and then `terraform state rm <resource-name>` to remove the resources that have been manually deleted. TODO: fix this issue.

I have compiled a doc of common errors and fixes [here](https://docs.google.com/document/d/e/2PACX-1vSFvg2ef77R-OVmpaxK_70Fsc6lEvHpr3YM5G2eZOE8XLeQxtGZc-5yu-tTqhH3KEerSE3LCCzimkkS/pub).

# Appendix

## Knowledge

### Terraform

Terraform is a configuration language that allows for reproducible provisioning with cloud providers. You can run `terraform init` when setting up a new configuration, changing backend settings, or modifying provider versions. You don't need to run it for regular operations like `terraform plan` and `terraform apply` unless the configuration changes. Then, you can run `terraform plan` to see the plan generated by terraform for the specified deployment configuration. If you are satisfied with the plan, run `terraform apply` to apply the configuration. Run `terraform -help` to see further commands.

### Customizing variables

To customize the resource group name or any other variable, change the default value in the `infra/recipes/terraform/azure/variables.tf` file.

### Terraform Backend

Terraform backend state can be stored locally or remotely. In this case, the default is to store the state locally but it can also be configured to be stored in Azure blob store. To set up remote backend, see the `Storing backend files in Azure Storage` section.

## Storing backend files in Azure storage

### Creating a Storage Account

A storage account must be created to hold these backend terraform files. Create a new storage account in your Azure portal using an arbitrary resource group. It is typical to name these resources starting with `tfbackend`. Then, within this storage account, create a new blob container. Note the resource group name, storage account name, and blob container name. Also note the storage account key, which can be found in the "Access keys" sidebar option in the storage account. These variables will be used when configuring the backend.

### Terraform backend setup

Uncomment these lines in the `backend.tf` files in the given environment:

```
terraform {
  backend "azurerm" {}
}
```

This dictates that the state will now be stored in an Azure storage account. To configure this you must set the variables like those in `infra/recipes/terraform/azure/backend.tfvars.template`. These variables are the ones found when setting up the storage account. Ensure that each environment is associated with at least a separate storage container to avoid overwriting of state files.

You must also change the config setup in the [provider](infra/recipes/terraform/azure/provider.tf) file, in the `terraform_remote_state` block, to ensure that the unique ACR name is correctly imported to the sandbox environment. See [here](https://developer.hashicorp.com/terraform/language/state/remote-state-data#config) for more details.

### Configuring the backend with your Azure account

#### Using Environment Variables
You could set the environment variables for variables to stored terraform backend state. For example, to initialize `storage_account_name` you can set the following environment variable.
`export TF_VAR_storage_account_name="foobar"`

#### Using tfvars
1. Create a copy of the file `backend.tfvars.template` into your environment directory by running
`cp backend.tfvars.template ./environments/<environment-name>/backend.tfvars`.
2. Edit the backend.tfvars to configure the necessary TF backend state.

### Initializing the backend

_If using tfvars_
`terraform init -backend-config="./backend.tfvars"`

_If using environment variables_
`terraform init`