#!/bin/bash
############################################################
# Help                                                     #
############################################################
Help()
{
   echo "CodeGen shell utility that downloads openapi-cli jar"
   echo
   echo "Syntax: ./jar_download.sh [OPTIONS]"
   echo "options:"
   echo "-o=DIRECTORY    Output path to which the jar will download to"
   echo "-v=SEMVER_STR   Semantic version of the jar to use, for example 6.6.0"
   echo
}
############################################################
# Process the input options. Add options as needed.        #
############################################################
unset output
unset version

while getopts ":ho:v:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      o) output=$OPTARG;;
      v) version=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit 1;;
   esac
done

if [ -z "$output" ] || [ -z "$version" ]; then
        echo 'Missing -o or -v, see -h'
        exit 1;
fi

############################################################
# Main program                                             #
############################################################
echo "Downloading openapi generator JAR in bin folder ${output} if needed..."
mkdir -p "$output" || exit 1
cd "$output" || exit 1

# "-c" option avoid re-downloading the server's version has the same size as local.
# Alternatively, "-nc" which avoid re-downloading whenever the same file exists or "-N" that turns up timestamping
# are not the best solution here.
# See: https://stackoverflow.com/questions/4944295
FILE="openapi-generator-cli-${version}.jar"
if test -f "$FILE"; then
  echo "$FILE exists."
  exit 0
fi
echo "Downloading $FILE..."
curl -f "https://repo.maven.apache.org/maven2/org/openapitools/openapi-generator-cli/${version}/openapi-generator-cli-${version}.jar" --output "$FILE" || exit 1