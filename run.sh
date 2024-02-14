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

java -Xmx256M -Xms64M -XX:NativeMemoryTracking=summary -jar "$@"
