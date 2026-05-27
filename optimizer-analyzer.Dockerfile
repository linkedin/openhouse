FROM openjdk:23-ea-11-slim

ARG USER=openhouse
ARG USER_ID=1000
ARG GROUP_ID=1000
ENV APP_NAME=optimizer-analyzer
ENV USER_HOME=/home/$USER
ENV ANALYZER_INTERVAL_SECONDS=30

# Create an openhouse user as there's no reason to run as root user
RUN groupadd --force -g $GROUP_ID $USER && useradd -l -d $USER_HOME -m $USER -u $USER_ID -g $GROUP_ID

WORKDIR $USER_HOME

# IMAGE does not set the necessary paths by default.
ENV PATH=$PATH:/export/apps/jdk/JDK-1_8_0_172/bin/:$USER_HOME

ARG VERSION="1.0.0-SNAPSHOT"
ARG BUILD_DIR="build/analyzerapp/libs"
ARG JAR_FILES=$BUILD_DIR/*.jar

COPY $JAR_FILES ./

# Delete unwanted JAR files
RUN find . -name "*-sources.jar" -delete
RUN find . -name "*-javadoc.jar" -delete
RUN find . -name "*-lib.jar" -delete

# Rename the JAR file.
RUN find ./ -name "*.jar" -exec mv {} $APP_NAME.jar \;
RUN ls $APP_NAME.jar

# Ensure that everything in $USER_HOME is owned by openhouse user
RUN chown -R openhouse:openhouse $USER_HOME

# Setup default path for Java
RUN mkdir -p /usr/java && ln -sfn /export/apps/jdk/JDK-1_8_0_172 /usr/java/default

USER $USER

# Loop the analyzer on a fixed interval. Each pass is a fresh JVM, so opt-3's
# analyzer (which runs once per CommandLineRunner invocation and exits) becomes
# a continuous service in this container.
ENTRYPOINT ["sh", "-c", "while true; do echo \"Running $APP_NAME at $(date)\"; java -Xmx256M -Xms64M -XX:NativeMemoryTracking=summary -jar $APP_NAME.jar; echo \"Exited; sleeping ${ANALYZER_INTERVAL_SECONDS}s\"; sleep ${ANALYZER_INTERVAL_SECONDS}; done"]
