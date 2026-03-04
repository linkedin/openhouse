FROM python:3.12-slim-bookworm

# JDK for PyArrow's libhdfs JNI bridge
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Hadoop 2.8.5 client (matches the HDFS namenode/datanode images)
ENV HADOOP_VERSION=2.8.5
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /opt && \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

# uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --all-extras

COPY src/ src/
COPY tests/ tests/
