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

## Guarded SYSTEM_ONLY locks

[Guarded lock operations](guarded-system-locks.md) use the distinct
`/v1/databases/{databaseId}/tables/{tableId}/lock/SYSTEM_ONLY/guarded` route so older
servers cannot silently ignore generation or identity guards. POST requires
`expectedTableUUID` and a stable, caller-selected `lockId`; the server records the
authenticated `lockOwner`. DELETE requires all three values and preserves
nonmatching locks. Both operations retain existing `LOCK_ADMIN` authorization.
The original lock endpoints remain available for explicit owner/admin operations.

The focused reference is generated with the same Widdershins command above,
selecting this route and its `CreateGuardedLockRequestBody` and `LockState` schemas
from `build/tables/specs/tables.json`.