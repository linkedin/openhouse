FROM bde2020/hadoop-namenode:1.2.0-hadoop2.8-java8

# PyArrow's libhdfs JNI bridge needs CLASSPATH and the native library location.
# JAVA_HOME and HADOOP_HOME are already set by the base image (hadoop-2.8.0).
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
RUN uv python install 3.12

WORKDIR /app
COPY pyproject.toml uv.lock ./
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --all-extras

COPY src/ src/
COPY tests/ tests/
