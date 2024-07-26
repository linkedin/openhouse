#!/bin/bash
############################################################
# Help                                                     #
############################################################
Help()
{
   echo "CodeGen shell utility that generates java client code from an OpenApi spec"
   echo
   echo "Syntax: ./client_codegen.sh [OPTIONS]"
   echo "options:"
   echo "-i=FILE         Input path of OpenApi spec.json"
   echo "-o=DIRECTORY    Output path to generate the client code"
   echo "-j=DIRECTORY    Path to the directory containing openapi-generator-cli.jar"
   echo "-n=STRING       Name of the service, for example: housetables"
   echo "-v=SEMVER_STR   Semantic version of the jar to use, for example 6.6.0"
   echo
}
############################################################
# Process the input options. Add options as needed.        #
############################################################
unset input
unset output
unset jar
unset name
unset version

while getopts ":hn:i:o:j:v:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      n) name=$OPTARG;;
      i) input=$OPTARG;;
      o) output=$OPTARG;;
      j) jar=$OPTARG;;
      v) version=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit 1;;
   esac
done

if [ -z "$input" ] || [ -z "$output" ] || [ -z "$jar" ] || [ -z "$name" ] || [ -z "$version" ]; then
        echo 'Missing -i or -o or -n or -j or -v, see -h'
        exit 1;
fi

############################################################
# Main program                                             #
############################################################
echo "Generating code for ${name} at ${output} using spec ${input} and jar ${jar}/openapi-generator-cli-${version}.jar"
echo "using template overrides ${jar}/templates/auth-overrides"
rm -rf "$output"
java -jar "${jar}/openapi-generator-cli-${version}.jar" generate \
  -i "$input" \
  --api-package "com.linkedin.openhouse.${name}.client.api" \
  --model-package "com.linkedin.openhouse.${name}.client.model" \
  --invoker-package "com.linkedin.openhouse.${name}.client.invoker" \
  --group-id "com.linkedin.openhouse.${name}" \
  --artifact-id "generated-${name}-client" \
  --artifact-version 0.0.1-SNAPSHOT \
  -g java \
  -p java8=true \
  -p generateConstructorWithAllArgs=true \
  --library webclient \
  -o "$output"