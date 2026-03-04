FROM oh-spark-base

# PyArrow's libhdfs JNI bridge needs CLASSPATH and the native library location.
# JAVA_HOME and HADOOP_HOME are already set by the base image.
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
ENV ARROW_LIBHDFS_DIR="${HADOOP_HOME}/lib/native"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --all-extras

COPY src/ src/
COPY tests/ tests/
