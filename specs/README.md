# OpenHouse Specifications

## Introduction

All the specs for the OpenHouse services should be maintained in [specs](README.md) directory. The specs should be NOT
be editted manually. This document provides the steps to update the specs for the OpenHouse services.

### Pre-requisites

Install the [Widdershins](https://github.com/Mermade/widdershins) CLI to convert OpenAPI specification format to
markdown.
```
npm install -g widdershins
```

### Steps to update the specs

1. Make sure you have the latest version of the OpenHouse services.
2. Make updates to the API for OpenHouse services.
3. Pick the service spec you want to update. The first run the build command, followed by spec gen command:

| Service     | Build | SpecGenCmd                                                                                                                                         | Location                                 |
|-------------| --- |----------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| Catalogs    | `./gradlew clean :services:tables:generateOpenApiDocs` | `widdershins --omitHeader true --search true --language_tabs None --summary build/tables/specs/tables.json -o docs/specs/catalog.md`               | [Catalog](docs/specs/catalog.md)         |
| HouseTables | `./gradlew clean :services:housetables:generateOpenApiDocs` | `widdershins --omitHeader true --search true --language_tabs None --summary build/housetables/specs/housetables.json -o docs/specs/housetables.md` | [HouseTables](docs/specs/housetables.md) |
| Jobs        | `./gradlew clean :services:jobs:generateOpenApiDocs` | `widdershins --omitHeader true --search true --language_tabs None --summary build/jobs/specs/jobs.json -o docs/specs/jobs.md`                      | [Jobs](docs/specs/jobs.md)               |

4. Commit the changes to the specs directory.

You could also use the [script](scripts/spec_update.sh) to update the specs.