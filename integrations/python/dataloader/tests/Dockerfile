# Minimal runtime image with Java 8 + Hadoop 2.8 for HDFS access.
# Dependencies are pre-installed into build/itest-site-packages on the host and copied in.
FROM python:3.10-slim-bookworm

# Copy JRE 8 and Hadoop 2.8 client from the hadoop base image.
# JRE is needed by PyArrow's libhdfs JNI bridge to read HDFS data.
# Hadoop 2.8 requires Java 8 — it is incompatible with Java 17 (NoClassDefFoundError).
COPY --from=bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8 /usr/lib/jvm/java-8-openjdk-amd64/jre /opt/java
COPY --from=bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8 /opt/hadoop-2.8.0 /opt/hadoop-2.8.0

ENV JAVA_HOME=/opt/java
ENV HADOOP_HOME=/opt/hadoop-2.8.0
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

# Expand CLASSPATH globs at build time. JNI does not expand '*' wildcards the
# way the java launcher does, so we must list every jar explicitly.
RUN echo "${HADOOP_HOME}/etc/hadoop" > /tmp/cp_parts && \
    find ${HADOOP_HOME}/share/hadoop/common ${HADOOP_HOME}/share/hadoop/common/lib \
         ${HADOOP_HOME}/share/hadoop/hdfs ${HADOOP_HOME}/share/hadoop/hdfs/lib \
         -maxdepth 1 -name '*.jar' >> /tmp/cp_parts && \
    paste -sd ':' /tmp/cp_parts > /tmp/cp && \
    rm /tmp/cp_parts
ENV CLASSPATH_FILE=/tmp/cp

WORKDIR /app
COPY build/itest-site-packages/ /app/site-packages/
ENV PYTHONPATH=/app/site-packages
COPY tests/ tests/

# Set CLASSPATH from the expanded file at container start.
ENTRYPOINT ["sh", "-c", "export CLASSPATH=$(cat /tmp/cp) && exec \"$@\"", "--"]
