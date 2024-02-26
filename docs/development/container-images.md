# Container Images

- [Container Images](#container-images)
  - [Build container images locally](#build-and-run-container-images-locally)
  - [Prepare MP to produce container images](#prepare-mp-to-produce-container-images)
  - [Monitoring Metrics]()

## Build and Run container images locally

Dockerfile for each of the services are made available. Below are the example commands for Tables service
defined by tables-service.Dockerfile.

```  
    # Build Container Image for Tables Service
    docker build [--build-arg JAR_FILE=$JAR_FILE | --tag $REPOSITORY:$TAG] --file tables-service.Dockerfile .
    
    # Run the Container Image for Tables Services
    docker run [--env OPENHOUSE_CLUSTER_CONFIG_PATH=$[FILE_PATH]] [--interactive --tty | --detach] -p [$HOST_IP]$HOST_PORT:8080 $REPOSITORY:$TAG
```

## Monitoring Metrics

Services can be configured to report metrics for operational monitoring using prometheus. OpenHouse uses prometheus
as the time-series database for storing metrics.