echo "Downloading openapi generator JAR in bin folder $1 if needed..."
mkdir -p $1 || exit 1
cd $1

# "-c" option avoid re-downloading the server's version has the same size as local.
# Alternatively, "-nc" which avoid re-downloading whenever the same file exists or "-N" that turns up timestamping
# are not the best solution here.
# See: https://stackoverflow.com/questions/4944295
FILE=openapi-generator-cli.jar
if test -f "$FILE"; then
  echo "$FILE at version 5.3.0 exists."
  exit
fi
curl https://repo.maven.apache.org/maven2/org/openapitools/openapi-generator-cli/5.3.0/openapi-generator-cli-5.3.0.jar --output openapi-generator-cli.jar