#!/bin/sh

echo "Command $@"

if [ "$#" -lt 1 ]; then
    echo "Script accepts at least Executable Jar"
    exit 1
fi

echo "Executing Jar $1 at "
date

# OpenTelemetry Java Agent for distributed tracing.
# Attaches the agent if the JAR exists and OTEL_TRACES_ENABLED is not explicitly "false".
# Fine-grained control (sampling, exporters, etc.) is via standard OTEL_* env vars.
OTEL_AGENT_JAR="./otel/opentelemetry-javaagent.jar"
OTEL_AGENT_OPTS=""
if [ -f "$OTEL_AGENT_JAR" ] && [ "${OTEL_TRACES_ENABLED:-true}" != "false" ]; then
    echo "Attaching OpenTelemetry Java Agent from $OTEL_AGENT_JAR"
    OTEL_AGENT_OPTS="-javaagent:${OTEL_AGENT_JAR}"
fi

# Using -XX:NativeMemoryTracking=summary for quick idea on java process memory breakdown, one could switch to
# "detail" for further details

java $OTEL_AGENT_OPTS -Xmx256M -Xms64M -XX:NativeMemoryTracking=summary -jar "$@"
