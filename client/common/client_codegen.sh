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
   echo
}
############################################################
# Process the input options. Add options as needed.        #
############################################################
unset input
unset output
unset jar
unset name

while getopts ":hn:i:o:j:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      n) name=$OPTARG;;
      i) input=$OPTARG;;
      o) output=$OPTARG;;
      j) jar=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done

if [ -z "$input" ] || [ -z "$output" ] || [ -z "$jar" ] || [ -z "$name" ]; then
        echo 'Missing -i or -o or -n or -j, see -h'
        exit;
fi

############################################################
# Main program                                             #
############################################################
echo "Generating code for $name at $output using spec $input and jar $jar/openapi-generator-cli.jar"
rm -rf $output
java -jar $jar/openapi-generator-cli.jar generate \
  -i $input \
  --api-package com.linkedin.openhouse.$name.client.api \
  --model-package com.linkedin.openhouse.$name.client.model \
  --invoker-package com.linkedin.openhouse.$name.client.invoker \
  --group-id com.linkedin.openhouse.$name \
  --artifact-id generated-$name-client \
  --artifact-version 0.0.1-SNAPSHOT \
  -g java \
  -p java8=true \
  --library webclient \
  -o $output
