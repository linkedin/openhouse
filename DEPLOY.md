<html>
  <div align="center">
    <img src="docs/images/openhouse-logo.jpeg" alt="OpenHouse" width="400" height="300">
  </div>
</html>

Use this guide to deploy OpenHouse services to Kubernetes cluster.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Kubernetes](https://kubernetes.io/docs/setup/)
- [Helm](https://helm.sh/docs/intro/install/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

## Overview

Below we discuss the structure of the Kubernetes and Helm for deploying OpenHouse.

```
infra/recipes/k8s
├── config --> Service specific configuration files (e.g. cluster.yaml)
│   ├── tables --> Configuration files for Catalog service.
│   │   ├── minikube --> Configuration files for minikube cluster
│   │   ├── ... --> Configuration files for custom cluster
│   ├── ... --> Configuration files for other services
│   │   ├── ... --> Similar layout for configuration files for minikube or other cluster
├── templates --> k8s YAML templates such as ConfigMap, Deployment, Service, etc.
│   ├── tables --> k8s spec for tables service: configmap, deployment, service.
│   ├── ... --> k8s spec for other services: configmap, deployment, service.
├── environments --> Environment specific configuration files for overriding default configurations.
│   ├── minikube --> Configuration files for minikube cluster
│   ├── ... --> Configuration files for other clusters
├── helm--> Helm charts for deploying OpenHouse services.
│   ├── tables --> Helm chart for tables service.

```

## Deploying to MiniKube

### Build container images

We build the container images for the services using Docker and make them available to MiniKube. To do this,
run the following command.

```
eval $(minikube docker-env)
```

From the root directory of the repository, run the following command to build the container images.

```
docker build -t openhouse-tables-service:latest -f tables-service.Dockerfile .
docker build -t openhouse-housetables-service:latest -f housetables-service.Dockerfile .
```

### Deploy services

We use Helm to deploy the services to MiniKube. To do this, run the following commands from the [helm](infra/recipes/k8s/helm)
directory.

To deploy the catalog service:
```
helm install openhouse-tables-service tables -f ../environments/minikube/tables/values.yaml
```

To deploy the house table service:
```
helm install openhouse-housetables-service housetables -f ../environments/minikube/housetables/values.yaml
```

In case an existing deployment needs to be updated, use the following command.
```
helm upgrade openhouse-tables-service tables -f ../environments/minikube/tables/values.yaml
helm upgrade openhouse-housetables-service housetables -f ../environments/minikube/housetables/values.yaml
```

Check if the deployment succeeded, by running the following command.
```
helm list
kubectl get deployments
kubectl get pods
```

### Accessing the services

To access Catalog (tables) service we need to open a tunnel. To do this, run the following command.
```
 minikube service openhouse-tables-service
```

URL for the service will be displayed in the terminal. Use this URL to access the service.
```
curl -vv -XGET http://"${url}"/v1/databases/db/tables
```

Make sure you get a HTTP 200 (OK) response. Try out more commands to test the service as described in the [SETUP.md](SETUP.md)

### Default Configurations

The configurations for the services are stored in the [config](infra/recipes/k8s/config) directory.
File [cluster.yaml](infra/recipes/k8s/config/tables/minikube/tables.yaml) contains the configuration used by the
Catalog (tables) service and House Tables service. For minikube environment, we use the following configuration for the
services:

**Authentication and Authorization is disabled**

```yaml
  security:
    token:
      interceptor:
        classname: null
    tables:
      authorization:
        enabled: false
```

**House Table Service uses in-memory database**

```yaml
  housetables:
    base-uri: "http://openhouse-housetables-service:8080"
    database:
      type: "IN_MEMORY"
```

**Local File System is used for storing the data**

```yaml
  storage:
    type: "hadoop"
    uri: "file:///"
    root-path: "/tmp/minikube_openhouse"
```

### Custom Configurations

#### MySQL for HouseTables Service

To use MySQL, update [cluster.yaml](infra/recipes/k8s/config/housetables/minikube/housetables.yaml). Sample
configuration for MySQL is shown below.
    
```yaml
  housetables:
    base-uri: "http://openhouse-housetables-service:8080"
    database:
      type: "MYSQL"
      url: "jdbc:mysql://mysql:3306/oh_db"
```

MySQL database might need credentials in such cases use k8s secrets. These secrets should be made available to House Table
Service, under the name `openhouse-housetables-service-secrets`. Sample command to create the secrets is
shown below.

```
kubectl create secret generic openhouse-housetables-service-secrets --from-literal=MYSQL_PASSWORD=oh_password --from-literal=MYSQL_USER=oh_user
```

Finally, to make k8s deployment aware of above secrets, update the environment specific configurations in 
[environments](infra/recipes/k8s/environments/minikube) directory accordingly. Sample configuration that is overridden
during `helm install` is shown below.

```yaml
  mysql:
    enabled: true
    secrets:
      HTS_DB_USER: MYSQL_USER
      HTS_DB_PASSWORD: MYSQL_PASSWORD
```

#### Hadoop Compatible File System for Tables Service

To use remote Hadoop compatible filesystem, update the `storage.uri` in [cluster.yaml](infra/recipes/k8s/config/tables/minikube/tables.yaml)

```yaml
  storage:
    type: "hadoop"
    uri: "hdfs://namenode:9000"
    root-path: "/data/openhouse"
```