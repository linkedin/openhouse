FROM python:3.12-slim-bookworm

# JRE is needed by PyArrow's libhdfs JNI bridge to read HDFS data
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*

# Copy Hadoop client JARs and native libraries from the hadoop base image.
# Only the HDFS client is needed — Spark/Livy run in separate containers.
COPY --from=bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8 /opt/hadoop-2.8.0 /opt/hadoop-2.8.0

ENV HADOOP_HOME=/opt/hadoop-2.8.0
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --all-extras

COPY src/ src/
COPY tests/ tests/
