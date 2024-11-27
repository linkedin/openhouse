#!/bin/sh

echo "Command $@"

if [[ "$#" -lt 1 ]]; then
    echo "Script accepts at least Executable Jar"
    exit 1
fi

echo "Executing Jar $1 at "
date

# Using -XX:NativeMemoryTracking=summary for quick idea on java process memory breakdown, one could switch to
# "detail" for further details

java -Xmx256M -Xms64M -XX:NativeMemoryTracking=summary -Djavax.net.ssl.keyStore=/var/config/identity.p12 -Djavax.net.ssl.keyStorePassword=work_around_jdk-6879539 -Djavax.net.ssl.trustStore=/etc/riddler/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.keyStoreType=PKCS12 -Dcom.amazonaws.services.s3.disablePutObjectMD5Validation=true -Dcom.amazonaws.services.s3.disableGetObjectMD5Validation=true -Dcom.amazonaws.sdk.disableCertChecking=true -Dlog4j.configuration=file:/var/config/log4j-config.xml -Dlog4j1.compatibility=true -Djavax.net.debug=ssl -jar "$@"
