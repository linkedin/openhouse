#!/bin/bash

ACCOUNT_KEY=$(terraform output -raw storage_account_key)
ACCOUNT_NAME=$(terraform output -raw storage_account_name)
RESOURCE_GROUP=$(terraform output -raw resource_group_name)
CLUSTER_NAME=$(terraform output -raw aks_cluster_name)

az aks get-credentials --resource-group ${RESOURCE_GROUP} --name ${CLUSTER_NAME}

TABLES_ADDRESS=$(kubectl get service openhouse-tables-service -o jsonpath="{.status.loadBalancer.ingress[0].ip}")
TABLES_PORT=$(kubectl get service openhouse-tables-service -o jsonpath="{.spec.ports[*].port}")

COMMAND="bin/spark-shell --packages org.apache.iceberg:iceberg-azure:1.5.0,org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0 \
  --jars openhouse-spark-apps_2.12-*-all.jar,openhouse-spark-runtime_2.12-latest-all.jar  \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions   \
  --conf spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog   \
  --conf spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog     \
  --conf spark.sql.catalog.openhouse.metrics-reporter-impl=com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter    \
  --conf spark.sql.catalog.openhouse.uri=http://${TABLES_ADDRESS}:${TABLES_PORT}/  \
  --conf spark.sql.catalog.openhouse.auth-token=\$(cat /var/config/\$(whoami).token) \
  --conf spark.sql.catalog.openhouse.cluster=AKSCluster \
  --conf spark.sql.catalog.openhouse.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO \
  --conf spark.sql.catalog.openhouse.adls.auth.shared-key.account.name=${ACCOUNT_NAME} \
  --conf spark.sql.catalog.openhouse.adls.auth.shared-key.account.key=${ACCOUNT_KEY} \
  --conf spark.sql.catalogImplementation=in-memory"

echo $COMMAND > ./start-spark-shell.sh
chmod +x ./start-spark-shell.sh

docker cp ./start-spark-shell.sh local.spark-master:/opt/spark

docker exec -it local.spark-master /bin/bash -c ./start-spark-shell.sh

./start-spark-shell.sh