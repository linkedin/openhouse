FROM openjdk:23-ea-11-slim

ARG USER=openhouse
ARG USER_ID=1000
ARG GROUP_ID=1000
ENV APP_NAME=openhouse-spark-apps_2.12
ENV USER_HOME=/home/$USER

# Create an openhouse user as there's no reason to run as root user
RUN groupadd --force -g $GROUP_ID $USER && useradd -l -d $USER_HOME -m $USER -u $USER_ID -g $GROUP_ID

WORKDIR $USER_HOME

# IMAGE does not set the necessary paths by default.
ENV PATH=$PATH:/export/apps/jdk/JDK-1_8_0_172/bin/:$USER_HOME

ARG BUILD_DIR="build/$APP_NAME/libs"
ARG JAR_FILES=$BUILD_DIR/*-uber.jar

COPY $JAR_FILES ./

# Rename the JAR file.
RUN ls ./
RUN find ./ -name "*.jar" -exec mv {} $APP_NAME.jar \;

RUN ls $APP_NAME.jar

# Ensure that everything in $USER_HOME is owned by openhouse user
RUN chown -R openhouse:openhouse $USER_HOME

# Setup default path for Java
RUN mkdir -p /usr/java && ln -sfn /export/apps/jdk/JDK-1_8_0_172 /usr/java/default

RUN echo '#!/bin/sh' > /usr/local/bin/entrypoint.sh && \
    echo 'exec java -Xmx256M -Xms64M -XX:NativeMemoryTracking=summary -cp ${APP_NAME}.jar com.linkedin.openhouse.jobs.scheduler.JobsScheduler --tokenFile /var/config/openhouse.token "$@"' >> /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

USER $USER

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
