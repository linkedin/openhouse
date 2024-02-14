#!/bin/sh

set -e

# Check if widdershins is installed
widdershins -h 2> /dev/null
# Check if the command failed
if [ $? -ne 0 ]; then
    echo "An error occurred while running the script. Please make sure that widdershins is installed."
    echo "See the documentation in docs/specs/README.md for more information."
    exit 1
fi

# Update the spec file with the latest API updates.
# This script is intended to be run from the root of the repository.

echo "Updating the Catalog service spec"
./gradlew clean :services:tables:generateOpenApiDocs
widdershins --omitHeader true --search true --language_tabs None --summary build/tables/specs/tables.json -o docs/specs/catalog.md

echo "Updating the House Table service spec"
./gradlew clean :services:housetables:generateOpenApiDocs
widdershins --omitHeader true --search true --language_tabs None --summary build/housetables/specs/housetables.json -o docs/specs/housetables.md

echo "Updating the Jobs service spec"
./gradlew clean :services:jobs:generateOpenApiDocs
widdershins --omitHeader true --search true --language_tabs None --summary build/jobs/specs/jobs.json -o docs/specs/jobs.md

