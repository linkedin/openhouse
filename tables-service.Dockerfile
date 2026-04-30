FROM openjdk:23-ea-11-slim


ARG USER=openhouse
ARG USER_ID=1000
ARG GROUP_ID=1000
ENV APP_NAME=tables
ENV USER_HOME=/home/$USER

# Create an openhouse user as there's no reason to run as root user
RUN groupadd --force -g $GROUP_ID $USER && useradd -l -d $USER_HOME -m $USER -u $USER_ID -g $GROUP_ID

WORKDIR $USER_HOME

# IMAGE does not set the necessary paths by default.
ENV PATH=$PATH:/export/apps/jdk/JDK-1_8_0_172/bin/:$USER_HOME

ARG VERSION="1.0.0-SNAPSHOT"
ARG BUILD_DIR="build/$APP_NAME/libs"
ARG JAR_FILES=$BUILD_DIR/*.jar

COPY $JAR_FILES ./

# Delete unwanted JAR files
RUN ls ./
RUN find . -name "*-sources.jar" -delete
RUN find . -name "*-javadoc.jar" -delete
RUN find . -name "*-lib.jar" -delete

# Rename the JAR file.
RUN ls ./
RUN find ./ -name "*.jar" -exec mv {} $APP_NAME.jar \;

RUN ls $APP_NAME.jar

COPY run.sh .


# Ensure that everything in $USER_HOME is owned by openhouse user
RUN chown -R openhouse:openhouse $USER_HOME

# Setup default path for Java
RUN mkdir -p /usr/java && ln -sfn /export/apps/jdk/JDK-1_8_0_172 /usr/java/default

USER $USER

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "./run.sh $APP_NAME.jar $@"]
