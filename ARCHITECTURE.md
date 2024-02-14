<html>
  <div align="center">
    <img src="docs/static/images/openhouse-logo.jpeg" alt="OpenHouse" width="400" height="300">
  </div>
</html>

## Control Plane for Tables

Following figure shows how OpenHouse control plane fits into a broader open source data lakehouse deployment.

![High Level Overview](docs/static/images/openhouse-controlplane.jpeg)

### Catalog Service

The core of OpenHouse's control plane is a RESTful Table Service that provides secure and scalable table provisioning
and declarative metadata management.

At the core of the Catalog Service is the Table model. A Table is a logical representation of a dataset in the data.
It includes the following metadata:
- Identifiers (Cluster ID, Database ID, Table ID, Table URI, Table UUID)
- Schema (Column Name, Column Type, Column Comment)
- Time Partitioning and String Clustering (Column Name)
- Table Policies (Retention, Sharing, Tags)
- Table Type (Primary, Replica)
- Versioning (Table Version)
- Other Metadata (Creator, Created Time, Last Modified Time)

You can review the [Catalog Service API Spec](docs/specs/catalog.md) for more details.

#### Multi Table Format Support

Catalog Service APIs are designed to support multiple table formats. Currently, it supports Iceberg and POC for Delta is
WIP. To be able to extend to multiple table formats, following considerations need to be made:

1. Base Table API, Metadata and Policies are designed to be format agnostic. For example, Retention, Sharing are generic
use-cases across all table formats.
2. Schema representation is serialized string. This allows to use the [Iceberg Schema](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/Schema.java)
   for Iceberg tables and for Delta tables use the [Spark StructType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/StructType.html)
3. In case there is a need to represent format specific metadata in the API, it is done through the API extensions as
   shown below:
```
/dabatase/{databaseId}/table/{tableId}/ --> Base Table API
/database/{databaseId}/table/{tableId}/iceberg/v2/snapshots --> Iceberg specific API extensions
/databases/{databaseId}/tables/{tableId}/delta/v1/actions --> Delta specific API extensions
```

#### Committing Table Metadata

When a table is created or updated, catalog service writes the table metadata to the table format specific metadata
file on storage. In case of Iceberg tables, the service writes all the Openhouse specific table metadata to Iceberg
table's root metadata json file. The service then writes the location of this root metadata to the House Table Service
through an atomic compare version and swap version and file location.

### House Table Service

House Table Service is a RESTful service designed to provide a key-value API for storing and retrieving
metadata for [Catalog Service](#catalog-service) and [Jobs Service](#jobs-service). The backend storage for House Table
Service is pluggable, and can use a Database supported by [Spring Data JPA](https://spring.io/projects/spring-data).
The default implementation uses H2 in-memory, but it can be extended to use other databases like MySQL, Postgres, etc.

You can review the [House Table Service Spec](docs/specs/housetables.md) for more details. Note, house table service is an internal
service, and should only be accessed via Catalog Service or Jobs Service.

### Data Services

Jobs Scheduler and Jobs Service are the two components that are responsible for orchestrating the execution of data
services. These components are designed to be modular and can be extended to support multiple data services.

#### Jobs Scheduler

[Jobs Scheduler](apps/spark) is responsible for iterating through all the tables, and for each table, it will trigger
the corresponding job type by calling the Jobs Service endpoint. It can be run as a cronjob per job type. Currently,
following job types are supported:
- Retention
- Iceberg Snapshot Expiration
- Iceberg Orphan File Deletion
- Iceberg Staged File Deletion

Jobs Scheduler integrates with the [Catalog Service](#catalog-service) to get the list of tables and corresponding
metadata. For example, if a table has a retention policy of 30 days, Jobs Scheduler will trigger the Job Service with
the table name and retention period.

#### Jobs Service

All the job types discussed in [Jobs Scheduler](#jobs-scheduler) are implemented as Spark applications, and it is the
[Jobs Service](services/jobs) that is responsible for submitting these jobs to the Spark cluster. Jobs Services is a
RESTful service that provides an endpoint for the scheduler to trigger a job. Jobs Service is modularized in a way that
it can be extended to support various spark job submission APIs. One such implementation based on Apache Livy can be
found [here](services/jobs), and it is used in docker compose setup.

Jobs Service also maintains all the job metadata in the [House Table Service](#house-table-service), including the
status of the job, the time it was triggered, table name, table metadata, error logs, and job type. This metadata is
used for tracking job completions, observability and monitoring purposes.

You can review the [Jobs Service Spec](docs/specs/jobs.md) for more details.

## Engine Integration

OpenHouse is designed to integrate with various engines like Spark, Trino, and Flink at their Catalog layer. For Iceberg
tables, [OpenHouseCatalog](integrations/spark) implementation is provided that extends Iceberg's [BaseMetastoreCatalog](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseMetastoreCatalog.java)
Given OpenHouse supports Table Sharing and Policy Management through Spark SQL syntax, SQL extensions are created, and
implemented in the OpenHouseCatalog.

The engine side integration with OpenHouseCatalog leverages a lower level REST client to interact with the Catalog
Service. This REST client is autogenerated as part of the build process from the OpenAPI documentation as described in
the [Client Codegen](docs/development/client-code-generation.md) guide.

Trino and Flink engines follow a similar pattern of integration with OpenHouse and are work in progress.

## Deployed System Architecture

![Example Deployed System Architecture](docs/static/images/openhouse-deployed-architecture.jpeg)

The figure above shows system components of OpenHouse deployed at LinkedIn. Each component is numbered and its purpose
is as follows:

[1] Catalog (/Table) service: This is a RESTful web service that exposes tables REST resources. This service is deployed
on a Kubernetes cluster with a fronting Envoy Network Proxy.

[2] REST clients: A variety of applications use REST clients to call into table service (#1). Clients include but are
not limited to compliance apps, replication apps, data discovery apps like Datahub and IaC, Terraform providers, and
data quality checkers. Some of the apps that work on all the tables in OpenHouse are assigned higher privileges.

[3] Metastore Catalog: Spark,Trino, andFlink engines are a special flavor of REST clients. An OpenHouse specific
metastore catalog implementation allows engines to integrate with OpenHouse tables.

[4] House Database (/Table) service: This is an internal service to store table service and data service metadata. This
service exposes a key-value interface that is designed to use a NoSQL DB for scale and cost optimization. However the
deployed system is currently backed by a MySQL instance, for ease of development and deployment.

[5] Managed namespace: This is a managed HDFS namespace where tables are persisted in Iceberg table format. Table
service is responsible for setting up the table directory structure with appropriate FileSystem permissioning.
OpenHouse has a novel HDFS permissioning scheme that makes it possible for any ETL flow to publish directly to Iceberg tables and securely into a managed HDFS namespace.

[6] Data services: This is a set of data services that reconciles the user / system declared configuration with the
system observed configuration. This includes use cases such as retention, restatement, and Iceberg-specific maintenance.
Each maintenance activity is scheduled as a Spark job per table. A Kubernetes cronjob is run periodically on a schedule to trigger a maintenance activity. All the bookkeeping of jobs is done in House Database Service using a jobs metadata table for ease of debugging and monitoring.

## Pluggable Architecture

Components of OpenHouse are designed to be pluggable, so that OpenHouse can be deployed in diverse private and public
cloud environments. This pluggability is available for the following components (Note: Integrations with a (+) are
available, and for other integrations, APIs are defined but not implemented yet.)
- Storage backend for example, HDFS(+), Blob Stores, etc.
- File Formats for example, Orc(+), Parquet(+).
- Database for [House Table Service](#house-table-service) for example, MySQL(+), Postgres, etc.
- Job Submission API for [Jobs Service](#jobs-service) for example, Apache Livy(+), Spark REST etc.
- Custom Authentication and Authorization handlers for all the services specific to an environment.

OpenHouse [Cluster Configuration Spec](cluster/configs) is the place where all the configurations are defined for a
particular cluster setup.

## Repository Layout

Main modules for OpenHouse are described below:
```
openhouse
├── apps --> Data Services for running Retention, and Iceberg maintenance jobs.
├── client --> REST clients for calling into the tables, housetables, and jobs service. Autogenerated from OpenAPI spec.
├── cluster --> Cluster Configuration Spec for defining configurations for a particular cluster setup.
├── iceberg --> Iceberg integration.
├── infra --> Recipes to deploy OpenHouse.
├── integrations --> Engine integrations with OpenHouseCatalog for Spark and Java applications.
├── services --> RESTful services for Tables, HouseTables, and Jobs.
```

Code for Catalog Service is located in the following directory:
- [services/tables](services/tables) The RESTful Catalog service that exposes /tables REST resources.
- [client/tableclient](client/tableclient) The REST client that is used by other services to call into the table
service. This client is autogenerated from the OpenAPI documentation during build.
- [integrations/java](integrations/java) Catalog Implementation of Iceberg Tables for end-user java applications.
- [integrations/spark](integrations/spark) Catalog Implementation of Iceberg Tables for end-user spark applications.

Code for Data Service is located in the following directory:
- [services/jobs](services/jobs) The RESTful service that exposes /jobs REST resources.
- [apps/spark](apps/spark) Spark applications for retention, and Iceberg maintenance jobs.
- [client/jobclient](client/jobclient) The REST client that is used by other services to call into the jobs service.
This client is autogenerated from the OpenAPI documentation during build.

Code for Iceberg integrations is located in the following directory:
- [iceberg/internalcatalog](iceberg/internalcatalog) Internal to OpenHouse, used by the RESTful Catalog service 
to persist Iceberg table metadata file. 
- [iceberg/htscatalog](iceberg/htscatalog) Shim layer that is used by Catalog Service and Data Service to interact with
House Table Service.

Code for House Table Service is located in the following directory:
- [services/housetables](services/housetables) The RESTful service that exposes /hts REST endpoint, used to store
User Iceberg Table and Jobs Table entities.
- [services/hts](services/hts) The REST client that is used by other services to call into the house table service.
  This client is autogenerated from the OpenAPI documentation during build.

Code for OpenHouse Cluster Configuration is located in the following directory:
- [cluster/configs](cluster/configs) The directory contains all the configurations for a particular cluster setup.
