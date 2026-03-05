FROM python:3.12-slim-bookworm

# Copy JRE 8 and Hadoop 2.8 client from the hadoop base image.
# JRE is needed by PyArrow's libhdfs JNI bridge to read HDFS data.
# Hadoop 2.8 requires Java 8 — it is incompatible with Java 17 (NoClassDefFoundError).
COPY --from=bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8 /usr/lib/jvm/java-8-openjdk-amd64/jre /opt/java
COPY --from=bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8 /opt/hadoop-2.8.0 /opt/hadoop-2.8.0

ENV JAVA_HOME=/opt/java

ENV HADOOP_HOME=/opt/hadoop-2.8.0
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

WORKDIR /app
COPY dist/itest.pyz ./
COPY tests/ tests/
