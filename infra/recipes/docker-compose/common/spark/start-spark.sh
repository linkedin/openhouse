#!/bin/bash

. "/opt/spark/bin/load-spark-env.sh"

if [ "$SPARK_WORKLOAD" == "master" ]; then
  SPARK_MASTER_HOST=`hostname`
  cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.master.Master --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG
elif [ "$SPARK_WORKLOAD" == "worker" ]; then
  cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.worker.Worker --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG
elif [ "$SPARK_WORKLOAD" == "submit" ]; then
    echo "SPARK SUBMIT"
elif [ "$SPARK_WORKLOAD" == "livy" ]; then
  CONF_FILE="${LIVY_HOME}/conf/livy.conf"
  if [[ -n "${SPARK_MASTER}" ]]; then
    echo "livy.spark.master=${SPARK_MASTER}" >> "${CONF_FILE}"
  fi
  if [[ -n "${LIVY_PORT}" ]]; then
    echo "livy.server.port=${LIVY_PORT}" >> "${CONF_FILE}"
  fi
  "$LIVY_HOME/bin/livy-server" $@
else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, livy, submit"
fi