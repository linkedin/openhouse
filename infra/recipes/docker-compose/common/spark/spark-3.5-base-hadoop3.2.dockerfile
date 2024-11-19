FROM openjdk:11.0.11-jdk-slim-buster as builder

RUN apt-get update && apt-get install -y \
    git curl vim zip software-properties-common ssh net-tools ca-certificates \
    # Add Dependencies for PySpark \
    python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.5.2 \
LIVY_VERSION=0.8.0-incubating-SNAPSHOT \
LIVY_HOME=/opt/livy \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
MAVEN_VERSION=3.9.4 \
PYTHONHASHSEED=1

# install apache spark
RUN curl --no-verbose -o apache-spark.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
  && mkdir -p /opt/spark \
  && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
  && rm apache-spark.tgz

# install maven to build apache livy
RUN curl --no-verbose -o maven.tgz https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && mkdir -p /opt/maven \
  && tar -xf maven.tgz -C /opt/maven --strip-components=1 \
  && rm maven.tgz

# build with patch and install apache livy
ARG ARCHIVE_FILENAME="apache-livy-${LIVY_VERSION}-bin"
COPY /infra/recipes/docker-compose/common/spark/livy_spark3_hadoop3.patch /
RUN git clone https://github.com/apache/incubator-livy \
    && cd incubator-livy \
    && git checkout 4d8a912699683b973eee76d4e91447d769a0cb0d \
    && git apply /livy_spark3_hadoop3.patch \
    && rm /livy_spark3_hadoop3.patch \
    && /opt/maven/bin/mvn clean package -B -V -e -Pspark-3.0 -Pthriftserver -DskipTests -DskipITs -Dmaven.javadoc.skip=true \
    && unzip -qq ./assembly/target/${ARCHIVE_FILENAME}.zip -d /opt \
    && mv "/opt/${ARCHIVE_FILENAME}" "${LIVY_HOME}" \
    && rm -rf "/incubator-livy"


FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

ENV LIVY_HOME=/opt/livy \
SPARK_HOME=/opt/spark

COPY --from=builder /opt/livy /opt/livy
COPY --from=builder /opt/spark /opt/spark

WORKDIR $SPARK_HOME

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=$SPARK_HOME/logs
ENV SPARK_MASTER_LOG=$SPARK_LOG_DIR/spark-master.out \
SPARK_WORKER_LOG=$SPARK_LOG_DIR/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077"
ENV OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318" \
OTEL_EXPORTER_OTLP_METRICS_PROTOCOL="http"

EXPOSE 8080 7077 7000 8998

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

WORKDIR $LIVY_HOME

RUN mkdir -p "${LIVY_HOME}/logs"

COPY /infra/recipes/docker-compose/common/spark/start-spark.sh /
COPY /build/openhouse-spark-3.5-runtime_2.12/libs/openhouse-spark-3.5-runtime_2.12-uber.jar $SPARK_HOME/openhouse-spark-runtime_2.12-latest-all.jar
COPY /build/openhouse-spark-apps_2.12/libs/openhouse-spark-apps_2.12-uber.jar $SPARK_HOME/openhouse-spark-apps_2.12-latest-all.jar
COPY /build/dummytokens/libs/dummytokens*.jar /dummytokens.jar
RUN java -jar /dummytokens.jar -d /var/config/

ARG OH_USERNAME=openhouse
ARG OH_GROUPNAME=$OH_USERNAME
ARG OH_USER_ID=1000
ARG OH_GROUP_ID=$OH_USER_ID
ENV OH_USER_HOME=/home/$OH_USERNAME

# Create an openhouse user as there's no reason to run as root user
RUN groupadd --force -g $OH_GROUP_ID $OH_USERNAME
RUN useradd -d $OH_USER_HOME -m $OH_USERNAME -u $OH_USER_ID -g $OH_GROUP_ID

RUN chown -R $OH_USERNAME:$OH_GROUPNAME /opt

ARG TABLE_OWNER_USERNAME=u_tableowner
ARG TABLE_OWNER_GROUPNAME=$TABLE_OWNER_USERNAME
ARG TABLE_OWNER_USER_ID=1001
ARG TABLE_OWNER_GROUP_ID=$TABLE_OWNER_USER_ID
ENV TABLE_OWNER_USER_HOME=/home/$TABLE_OWNER_USERNAME

# Create an openhouse user as there's no reason to run as root user
RUN groupadd --force -g $TABLE_OWNER_GROUP_ID $TABLE_OWNER_USERNAME
RUN useradd -d $TABLE_OWNER_USER_HOME -m $TABLE_OWNER_USERNAME -u $TABLE_OWNER_USER_ID -g $TABLE_OWNER_GROUP_ID


USER $OH_USERNAME

WORKDIR /opt/spark

CMD ["/bin/bash", "/start-spark.sh"]
