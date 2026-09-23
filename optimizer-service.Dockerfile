FROM eclipse-temurin:17-jre-jammy

ARG APP_NAME=optimizer
ARG USER=openhouse
ARG USER_ID=1000
ARG GROUP_ID=1000

RUN groupadd --force -g "$GROUP_ID" "$USER" \
    && useradd -l -m -u "$USER_ID" -g "$GROUP_ID" "$USER"

WORKDIR /home/$USER

# APP_NAME selects the REST service, analyzerapp, or schedulerapp bootJar.
COPY --chown=$USER:$USER build/${APP_NAME}/libs/${APP_NAME}.jar app.jar

ENV JAVA_TOOL_OPTIONS="-Xmx256M -Xms64M -XX:NativeMemoryTracking=summary"

USER $USER
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
