# Project structure and description

This directory is intended to provide a customizable configuration for Microsoft Azure using Terraform.

## Directory structure

### Overall structure
    .
    ├── environments            # Various specific environment configurations for Azure
        ├── sandbox             # The sandbox environment
    ├── modules                 # Reusable Azure components that are pre-defined
        ├── k8s                 # The Azure kubernetes configurations and services
        ├── mysql               # The Azure MySQL configurations and services
        ├── storage             # The Azure storage configurations and services
        ├── vm                  # The Azure virtual machine and network configurations and services
    ├── provider.tf             # Specific variables used for the kubernetes provider configuration
    ├── provider.tf             # The global azure and other providers needed for the configurations
    ├── variables.tf            # Global variables that can be reused across modules/environments
    ├── versions.tf             # The Azure Resource Manager version
    └── README.md

### Module/environment structure

Each module and environment contains a structure that is similar to the following (with occasional other specific files):

    .
    ├── main.tf                 # The main terraform file that defines all the necessary services
    ├── variables.tf            # The module/environment specific variables
    ├── outputs.tf              # The output variables from the module/environment
    └── common_variables.tf     # A symlink from the global variables file

## Adding/editing modules/environments

To add a new module, create a new folder in the `/modules` directory with the same format as above. Define the services in `main.tf`, the local variables in `variables.tf`, and the outputs in `outputs.tf`. `main.tf` will be the primary place to edit/update modules. 

To add a new environment, create a new folder in the `/environments` directory with the same format as above. For each environment add a `README` file as well as optional other files such as a `backend.tf` file. See the `environments/sandbox/` environment for examples. Instead of adding numerous new resources to the `main.tf` file in an environment, add modules that have been previously defined to maximize code reusability.