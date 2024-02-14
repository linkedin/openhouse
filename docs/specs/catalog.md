<!-- Generator: Widdershins v4.0.1 -->

<h1 id="openhouse-tables-apis">OpenHouse Tables APIs v0.1</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

API description for OpenHouse Tables API

Base URLs:

* <a href="http://localhost:8080">http://localhost:8080</a>

<a href="http://swagger.io/terms">Terms of service</a>

License: <a href="http://springdoc.org">Apache 2.0</a>

<h1 id="openhouse-tables-apis-snapshot">Snapshot</h1>

## Puts Iceberg snapshots to Table

<a id="opIdputSnapshotsV0"></a>

> Code samples

`PUT /v0/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots`

Puts Iceberg snapshots to Table

> Body parameter

```json
{
  "baseTableVersion": "Base table version to apply the change to",
  "jsonSnapshots": [
    "string"
  ],
  "createUpdateTableRequestBody": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "clusterId": "my_cluster",
    "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},{\"id\":3,\"required\":true,\"name\":\"timestampColumn\",\"type\":\"timestamp\"}]}",
    "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
    "clustering": "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]",
    "tableProperties": {
      "key": "value"
    },
    "policies": {
      "retention": "{retention:{count:3, granularity: 'day'}}",
      "sharingEnabled": false,
      "columnTags": "{'colName': [PII, HC]}"
    },
    "stageCreate": false,
    "baseTableVersion": "string",
    "tableType": "PRIMARY_TABLE"
  }
}
```

<h3 id="puts-iceberg-snapshots-to-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|
|body|body|[IcebergSnapshotsRequestBody](#schemaicebergsnapshotsrequestbody)|true|none|

> Example responses

> 200 Response

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "tableUri": "my_cluster.my_database.my_table",
  "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
  "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
  "tableVersion": "string",
  "tableCreator": "bob",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
  "lastModifiedTime": 1651002318265,
  "creationTime": 1651002318265,
  "tableProperties": {
    "key": "value"
  },
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": [
    "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
  ],
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="puts-iceberg-snapshots-to-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Iceberg snapshot PUT: UPDATED|[GetTableResponseBody](#schemagettableresponsebody)|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Iceberg snapshot PUT: CREATED|[GetTableResponseBody](#schemagettableresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Iceberg snapshot PUT: BAD_REQUEST|[GetTableResponseBody](#schemagettableresponsebody)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Iceberg snapshot PUT: CONFLICT|[GetTableResponseBody](#schemagettableresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="openhouse-tables-apis-table">Table</h1>

## Get Table in a Database

<a id="opIdgetTableV1"></a>

> Code samples

`GET /v1/databases/{databaseId}/tables/{tableId}`

Returns a Table resource identified by tableId in the database identified by databaseId.

<h3 id="get-table-in-a-database-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|

> Example responses

> 200 Response

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "tableUri": "my_cluster.my_database.my_table",
  "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
  "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
  "tableVersion": "string",
  "tableCreator": "bob",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
  "lastModifiedTime": 1651002318265,
  "creationTime": 1651002318265,
  "tableProperties": {
    "key": "value"
  },
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": [
    "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
  ],
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="get-table-in-a-database-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Table GET: OK|[GetTableResponseBody](#schemagettableresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table GET: UNAUTHORIZED|[GetTableResponseBody](#schemagettableresponsebody)|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Table GET: FORBIDDEN|[GetTableResponseBody](#schemagettableresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table GET: NOT_FOUND|[GetTableResponseBody](#schemagettableresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Update a Table

<a id="opIdupdateTableV1"></a>

> Code samples

`PUT /v1/databases/{databaseId}/tables/{tableId}`

Updates or creates a Table and returns the Table resources. If the table does not exist, it will be created. If the table exists, it will be updated.

> Body parameter

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},{\"id\":3,\"required\":true,\"name\":\"timestampColumn\",\"type\":\"timestamp\"}]}",
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]",
  "tableProperties": {
    "key": "value"
  },
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "stageCreate": false,
  "baseTableVersion": "string",
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="update-a-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|
|body|body|[CreateUpdateTableRequestBody](#schemacreateupdatetablerequestbody)|true|none|

> Example responses

> 200 Response

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "tableUri": "my_cluster.my_database.my_table",
  "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
  "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
  "tableVersion": "string",
  "tableCreator": "bob",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
  "lastModifiedTime": 1651002318265,
  "creationTime": 1651002318265,
  "tableProperties": {
    "key": "value"
  },
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": [
    "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
  ],
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="update-a-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Table PUT: UPDATED|[GetTableResponseBody](#schemagettableresponsebody)|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Table PUT: CREATED|[GetTableResponseBody](#schemagettableresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Table PUT: BAD_REQUEST|[GetTableResponseBody](#schemagettableresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table PUT: UNAUTHORIZED|[GetTableResponseBody](#schemagettableresponsebody)|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Table PUT: FORBIDDEN|[GetTableResponseBody](#schemagettableresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table PUT: DB_NOT_FOUND|[GetTableResponseBody](#schemagettableresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## DELETE Table

<a id="opIddeleteTableV1"></a>

> Code samples

`DELETE /v1/databases/{databaseId}/tables/{tableId}`

Deletes a table resource

<h3 id="delete-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|

<h3 id="delete-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Table DELETE: NO_CONTENT|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Table DELETE: BAD_REQUEST|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table DELETE: UNAUTHORIZED|None|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Table DELETE: FORBIDDEN|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table DELETE: TBL_DB_NOT_FOUND|None|

<aside class="success">
This operation does not require authentication
</aside>

## Search Tables in a Database

<a id="opIdsearchTablesV1"></a>

> Code samples

`POST /v1/databases/{databaseId}/tables/search`

Returns a list of Table resources present in a database. Only filter supported is 'database_id'.

<h3 id="search-tables-in-a-database-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "tableId": "my_table",
      "databaseId": "my_database",
      "clusterId": "my_cluster",
      "tableUri": "my_cluster.my_database.my_table",
      "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
      "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
      "tableVersion": "string",
      "tableCreator": "bob",
      "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
      "lastModifiedTime": 1651002318265,
      "creationTime": 1651002318265,
      "tableProperties": {
        "key": "value"
      },
      "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
      "clustering": [
        "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
      ],
      "policies": {
        "retention": "{retention:{count:3, granularity: 'day'}}",
        "sharingEnabled": false,
        "columnTags": "{'colName': [PII, HC]}"
      },
      "tableType": "PRIMARY_TABLE"
    }
  ]
}
```

<h3 id="search-tables-in-a-database-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Table SEARCH: OK|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Table SEARCH: BAD_REQUEST|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table SEARCH: UNAUTHORIZED|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Table SEARCH: FORBIDDEN|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table SEARCH: NOT_FOUND|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## List Tables in a Database

<a id="opIdgetAllTablesV1"></a>

> Code samples

`GET /v1/databases/{databaseId}/tables`

Returns a list of Table resources present in a database identified by databaseId.

<h3 id="list-tables-in-a-database-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "tableId": "my_table",
      "databaseId": "my_database",
      "clusterId": "my_cluster",
      "tableUri": "my_cluster.my_database.my_table",
      "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
      "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
      "tableVersion": "string",
      "tableCreator": "bob",
      "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
      "lastModifiedTime": 1651002318265,
      "creationTime": 1651002318265,
      "tableProperties": {
        "key": "value"
      },
      "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
      "clustering": [
        "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
      ],
      "policies": {
        "retention": "{retention:{count:3, granularity: 'day'}}",
        "sharingEnabled": false,
        "columnTags": "{'colName': [PII, HC]}"
      },
      "tableType": "PRIMARY_TABLE"
    }
  ]
}
```

<h3 id="list-tables-in-a-database-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Table GET_ALL: OK|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table GET_ALL: UNAUTHORIZED|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table GET_ALL: DB NOT_FOUND|[GetAllTablesResponseBody](#schemagetalltablesresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Create a Table

<a id="opIdcreateTableV1"></a>

> Code samples

`POST /v1/databases/{databaseId}/tables`

Creates and returns a Table resource in a database identified by databaseId

> Body parameter

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},{\"id\":3,\"required\":true,\"name\":\"timestampColumn\",\"type\":\"timestamp\"}]}",
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]",
  "tableProperties": {
    "key": "value"
  },
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "stageCreate": false,
  "baseTableVersion": "string",
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="create-a-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|body|body|[CreateUpdateTableRequestBody](#schemacreateupdatetablerequestbody)|true|none|

> Example responses

> 201 Response

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "tableUri": "my_cluster.my_database.my_table",
  "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
  "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
  "tableVersion": "string",
  "tableCreator": "bob",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
  "lastModifiedTime": 1651002318265,
  "creationTime": 1651002318265,
  "tableProperties": {
    "key": "value"
  },
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": [
    "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
  ],
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "tableType": "PRIMARY_TABLE"
}
```

<h3 id="create-a-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Table POST: CREATED|[GetTableResponseBody](#schemagettableresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Table POST: BAD_REQUEST|[GetTableResponseBody](#schemagettableresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Table POST: UNAUTHORIZED|[GetTableResponseBody](#schemagettableresponsebody)|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Table POST: FORBIDDEN|[GetTableResponseBody](#schemagettableresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table POST: DB_NOT_FOUND|[GetTableResponseBody](#schemagettableresponsebody)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Table POST: TBL_EXISTS|[GetTableResponseBody](#schemagettableresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Get AclPolicies for Table

<a id="opIdgetAclPoliciesV1"></a>

> Code samples

`GET /v1/databases/{databaseId}/tables/{tableId}/aclPolicies`

Returns principal to role mappings on Table resource identified by databaseId and tableId.

<h3 id="get-aclpolicies-for-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "principal": "string",
      "role": "string"
    }
  ]
}
```

<h3 id="get-aclpolicies-for-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|AclPolicies GET: OK|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|AclPolicies GET: BAD_REQUEST|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|AclPolicies GET: UNAUTHORIZED|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|AclPolicies GET: FORBIDDEN|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|AclPolicies GET: TABLE_NOT_FOUND|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Update AclPolicies for Table

<a id="opIdupdateAclPoliciesV1"></a>

> Code samples

`PATCH /v1/databases/{databaseId}/tables/{tableId}/aclPolicies`

Updates role for principal on Table identified by databaseId and tableId

> Body parameter

```json
{
  "role": "string",
  "principal": "string",
  "operation": "GRANT"
}
```

<h3 id="update-aclpolicies-for-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|tableId|path|string|true|Table ID|
|body|body|[UpdateAclPoliciesRequestBody](#schemaupdateaclpoliciesrequestbody)|true|none|

<h3 id="update-aclpolicies-for-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|AclPolicies PATCH: NO_CONTENT|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|AclPolicies PATCH: BAD_REQUEST|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|AclPolicies PATCH: UNAUTHORIZED|None|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|AclPolicies PATCH: FORBIDDEN|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|AclPolicies PATCH: TABLE_NOT_FOUND|None|

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="openhouse-tables-apis-database">Database</h1>

## Get AclPolicies on Database

<a id="opIdgetDatabaseAclPolicies"></a>

> Code samples

`GET /databases/{databaseId}/aclPolicies`

Returns principal to role mappings on resource identified by databaseId.

<h3 id="get-aclpolicies-on-database-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "principal": "string",
      "role": "string"
    }
  ]
}
```

<h3 id="get-aclpolicies-on-database-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|AclPolicies GET: OK|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|AclPolicies GET: BAD_REQUEST|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|AclPolicies GET: UNAUTHORIZED|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|AclPolicies GET: DB_NOT_FOUND|[GetAclPoliciesResponseBody](#schemagetaclpoliciesresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Update AclPolicies on database

<a id="opIdupdateDatabaseAclPoliciesV1"></a>

> Code samples

`PATCH /v1/databases/{databaseId}/aclPolicies`

Updates role for principal on database identified by databaseId

> Body parameter

```json
{
  "role": "string",
  "principal": "string",
  "operation": "GRANT"
}
```

<h3 id="update-aclpolicies-on-database-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|Database ID|
|body|body|[UpdateAclPoliciesRequestBody](#schemaupdateaclpoliciesrequestbody)|true|none|

<h3 id="update-aclpolicies-on-database-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|AclPolicies PATCH: UPDATED|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|AclPolicies PATCH: BAD_REQUEST|None|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|AclPolicies PATCH: UNAUTHORIZED|None|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|AclPolicies PATCH: FORBIDDEN|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|AclPolicies PATCH: DB_NOT_FOUND|None|

<aside class="success">
This operation does not require authentication
</aside>

## List all Databases

<a id="opIdgetAllDatabasesV1"></a>

> Code samples

`GET /v1/databases`

Returns a list of Database resources.

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "databaseId": "my_database",
      "clusterId": "my_cluster"
    }
  ]
}
```

<h3 id="list-all-databases-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Database GET_ALL: OK|[GetAllDatabasesResponseBody](#schemagetalldatabasesresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_ClusteringColumn">ClusteringColumn</h2>
<!-- backwards compatibility -->
<a id="schemaclusteringcolumn"></a>
<a id="schema_ClusteringColumn"></a>
<a id="tocSclusteringcolumn"></a>
<a id="tocsclusteringcolumn"></a>

```json
"\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"

```

Clustering columns for the table

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|columnName|string|true|none|Name of the clustering column in provided schema. The column should be of the type 'String'.Nested columns can also be provided with a dot-separated name (for example: 'eventHeader.countryCode').Column name is case-sensitive.|

<h2 id="tocS_CreateUpdateTableRequestBody">CreateUpdateTableRequestBody</h2>
<!-- backwards compatibility -->
<a id="schemacreateupdatetablerequestbody"></a>
<a id="schema_CreateUpdateTableRequestBody"></a>
<a id="tocScreateupdatetablerequestbody"></a>
<a id="tocscreateupdatetablerequestbody"></a>

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},{\"id\":3,\"required\":true,\"name\":\"timestampColumn\",\"type\":\"timestamp\"}]}",
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]",
  "tableProperties": {
    "key": "value"
  },
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "stageCreate": false,
  "baseTableVersion": "string",
  "tableType": "PRIMARY_TABLE"
}

```

Request containing details of the Table to be created

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|tableId|string|true|none|Unique Resource identifier for a table within a Database|
|databaseId|string|true|none|Unique Resource identifier for the Database containing the Table|
|clusterId|string|true|none|Unique Resource identifier for the Cluster containing the Database|
|schema|string|true|none|Schema of the table. OpenHouse tables use Iceberg schema specification|
|timePartitioning|[TimePartitionSpec](#schematimepartitionspec)|false|none|Time partitioning of the table|
|clustering|[[ClusteringColumn](#schemaclusteringcolumn)]¦null|false|none|Clustering columns for the table|
|tableProperties|object|true|none|Table properties|
|» **additionalProperties**|string|false|none|Table properties|
|policies|[Policies](#schemapolicies)|false|none|Policies of the table|
|stageCreate|boolean|false|none|Boolean that determines creating a staged table|
|baseTableVersion|string|true|none|The version of table that the current update is based upon|
|tableType|string|false|none|The type of a table|

#### Enumerated Values

|Property|Value|
|---|---|
|tableType|PRIMARY_TABLE|
|tableType|REPLICA_TABLE|

<h2 id="tocS_IcebergSnapshotsRequestBody">IcebergSnapshotsRequestBody</h2>
<!-- backwards compatibility -->
<a id="schemaicebergsnapshotsrequestbody"></a>
<a id="schema_IcebergSnapshotsRequestBody"></a>
<a id="tocSicebergsnapshotsrequestbody"></a>
<a id="tocsicebergsnapshotsrequestbody"></a>

```json
{
  "baseTableVersion": "Base table version to apply the change to",
  "jsonSnapshots": [
    "string"
  ],
  "createUpdateTableRequestBody": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "clusterId": "my_cluster",
    "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},{\"id\":3,\"required\":true,\"name\":\"timestampColumn\",\"type\":\"timestamp\"}]}",
    "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
    "clustering": "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]",
    "tableProperties": {
      "key": "value"
    },
    "policies": {
      "retention": "{retention:{count:3, granularity: 'day'}}",
      "sharingEnabled": false,
      "columnTags": "{'colName': [PII, HC]}"
    },
    "stageCreate": false,
    "baseTableVersion": "string",
    "tableType": "PRIMARY_TABLE"
  }
}

```

Request containing a list of JSON serialized Iceberg Snapshots to be put

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|baseTableVersion|string|true|none|Base Table Version|
|jsonSnapshots|[string]|false|none|List of json serialized snapshots to put|
|createUpdateTableRequestBody|[CreateUpdateTableRequestBody](#schemacreateupdatetablerequestbody)|false|none|Request containing details of the Table to be created|

<h2 id="tocS_Policies">Policies</h2>
<!-- backwards compatibility -->
<a id="schemapolicies"></a>
<a id="schema_Policies"></a>
<a id="tocSpolicies"></a>
<a id="tocspolicies"></a>

```json
{
  "retention": "{retention:{count:3, granularity: 'day'}}",
  "sharingEnabled": false,
  "columnTags": "{'colName': [PII, HC]}"
}

```

Policies of the table

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|retention|[Retention](#schemaretention)|false|none|Retention as required in /tables API request. The column holds the retention part or Policies.|
|sharingEnabled|boolean|false|none|Whether data sharing needs to enabled for the table in /tables API request. Sharing is disabled by default|
|columnTags|object|false|none|Policy tags applied to columns in /tables API request.|
|» **additionalProperties**|[PolicyTag](#schemapolicytag)|false|none|Policy tags applied to columns in /tables API request.|

<h2 id="tocS_PolicyTag">PolicyTag</h2>
<!-- backwards compatibility -->
<a id="schemapolicytag"></a>
<a id="schema_PolicyTag"></a>
<a id="tocSpolicytag"></a>
<a id="tocspolicytag"></a>

```json
"{'colName': [PII, HC]}"

```

Policy tags applied to columns in /tables API request.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|tags|[string]|false|none|Policy tags|

<h2 id="tocS_Retention">Retention</h2>
<!-- backwards compatibility -->
<a id="schemaretention"></a>
<a id="schema_Retention"></a>
<a id="tocSretention"></a>
<a id="tocsretention"></a>

```json
"{retention:{count:3, granularity: 'day'}}"

```

Retention as required in /tables API request. The column holds the retention part or Policies.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|count|integer(int32)|true|none|time period in count <granularity> for which the retention on table will be applied|
|granularity|string|true|none|time period granularity for which the retention on table will be applied|
|columnPattern|[RetentionColumnPattern](#schemaretentioncolumnpattern)|false|none|Optional object to specify retention column in case where timestamp is represented as a string|

#### Enumerated Values

|Property|Value|
|---|---|
|granularity|HOUR|
|granularity|DAY|
|granularity|MONTH|
|granularity|YEAR|

<h2 id="tocS_RetentionColumnPattern">RetentionColumnPattern</h2>
<!-- backwards compatibility -->
<a id="schemaretentioncolumnpattern"></a>
<a id="schema_RetentionColumnPattern"></a>
<a id="tocSretentioncolumnpattern"></a>
<a id="tocsretentioncolumnpattern"></a>

```json
"{columnName:datepartition, pattern: yyyy-MM-dd-HH}"

```

Optional object to specify retention column in case where timestamp is represented as a string

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|columnName|string|true|none|Name of retention column|
|pattern|string|true|none|Pattern for the value of the retention column following java.time.format.DateTimeFormatter standard.|

<h2 id="tocS_TimePartitionSpec">TimePartitionSpec</h2>
<!-- backwards compatibility -->
<a id="schematimepartitionspec"></a>
<a id="schema_TimePartitionSpec"></a>
<a id="tocStimepartitionspec"></a>
<a id="tocstimepartitionspec"></a>

```json
"\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}"

```

Time partitioning of the table

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|columnName|string|true|none|Name of the timestamp column in provided schema. The column should be of the type 'Timestamp'. Nested columns can also be provided with a dot-separated name (for example: 'eventHeader.timeColumn').Column name is case-sensitive.|
|granularity|string|true|none|Granularity of the time partition.|

#### Enumerated Values

|Property|Value|
|---|---|
|granularity|HOUR|
|granularity|DAY|
|granularity|MONTH|
|granularity|YEAR|

<h2 id="tocS_GetTableResponseBody">GetTableResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemagettableresponsebody"></a>
<a id="schema_GetTableResponseBody"></a>
<a id="tocSgettableresponsebody"></a>
<a id="tocsgettableresponsebody"></a>

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "clusterId": "my_cluster",
  "tableUri": "my_cluster.my_database.my_table",
  "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
  "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
  "tableVersion": "string",
  "tableCreator": "bob",
  "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
  "lastModifiedTime": 1651002318265,
  "creationTime": 1651002318265,
  "tableProperties": {
    "key": "value"
  },
  "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
  "clustering": [
    "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
  ],
  "policies": {
    "retention": "{retention:{count:3, granularity: 'day'}}",
    "sharingEnabled": false,
    "columnTags": "{'colName': [PII, HC]}"
  },
  "tableType": "PRIMARY_TABLE"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|tableId|string|false|read-only|Unique Resource identifier for a table within a Database|
|databaseId|string|false|read-only|Unique Resource identifier for the Database containing the Table|
|clusterId|string|false|read-only|Unique Resource identifier for the Cluster containing the Database|
|tableUri|string|false|read-only|Fully Qualified Resource URI for the table|
|tableUUID|string|false|read-only|Table UUID|
|tableLocation|string|false|read-only|Location of Table in File System / Blob Store|
|tableVersion|string|false|read-only|Current Version of the Table.|
|tableCreator|string|false|read-only|Authenticated user principal that created the Table.|
|schema|string|false|read-only|Schema of the Table in Iceberg|
|lastModifiedTime|integer(int64)|false|read-only|Last modification epoch time in UTC measured in milliseconds of a table.|
|creationTime|integer(int64)|false|read-only|Table creation epoch time measured in UTC in milliseconds of a table.|
|tableProperties|object|false|read-only|A map of table properties|
|» **additionalProperties**|string|false|none|A map of table properties|
|timePartitioning|[TimePartitionSpec](#schematimepartitionspec)|false|none|Time partitioning of the table|
|clustering|[[ClusteringColumn](#schemaclusteringcolumn)]¦null|false|none|Clustering columns for the table|
|policies|[Policies](#schemapolicies)|false|none|Policies of the table|
|tableType|string|false|read-only|The type of a table|

#### Enumerated Values

|Property|Value|
|---|---|
|tableType|PRIMARY_TABLE|
|tableType|REPLICA_TABLE|

<h2 id="tocS_GetAllTablesResponseBody">GetAllTablesResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemagetalltablesresponsebody"></a>
<a id="schema_GetAllTablesResponseBody"></a>
<a id="tocSgetalltablesresponsebody"></a>
<a id="tocsgetalltablesresponsebody"></a>

```json
{
  "results": [
    {
      "tableId": "my_table",
      "databaseId": "my_database",
      "clusterId": "my_cluster",
      "tableUri": "my_cluster.my_database.my_table",
      "tableUUID": "73ea0d21-3c89-4987-a6cf-26e4f86bdcee",
      "tableLocation": "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json",
      "tableVersion": "string",
      "tableCreator": "bob",
      "schema": "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"}]}",
      "lastModifiedTime": 1651002318265,
      "creationTime": 1651002318265,
      "tableProperties": {
        "key": "value"
      },
      "timePartitioning": "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}",
      "clustering": [
        "\"clustering\":[{\"columnName\":\"country\"},{\"columnName\":\"city\"}]"
      ],
      "policies": {
        "retention": "{retention:{count:3, granularity: 'day'}}",
        "sharingEnabled": false,
        "columnTags": "{'colName': [PII, HC]}"
      },
      "tableType": "PRIMARY_TABLE"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|[[GetTableResponseBody](#schemagettableresponsebody)]|false|read-only|List of Table objects in a database|

<h2 id="tocS_UpdateAclPoliciesRequestBody">UpdateAclPoliciesRequestBody</h2>
<!-- backwards compatibility -->
<a id="schemaupdateaclpoliciesrequestbody"></a>
<a id="schema_UpdateAclPoliciesRequestBody"></a>
<a id="tocSupdateaclpoliciesrequestbody"></a>
<a id="tocsupdateaclpoliciesrequestbody"></a>

```json
{
  "role": "string",
  "principal": "string",
  "operation": "GRANT"
}

```

Request containing aclPolicies of the Database to be updated

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|role|string|true|none|Role that is being granted/revoked.|
|principal|string|true|none|Grantee principal whose role is being updated|
|operation|string|true|none|Whether this is a grant/revoke request|

#### Enumerated Values

|Property|Value|
|---|---|
|operation|GRANT|
|operation|REVOKE|

<h2 id="tocS_AclPolicy">AclPolicy</h2>
<!-- backwards compatibility -->
<a id="schemaaclpolicy"></a>
<a id="schema_AclPolicy"></a>
<a id="tocSaclpolicy"></a>
<a id="tocsaclpolicy"></a>

```json
{
  "principal": "string",
  "role": "string"
}

```

List of acl policies associated with table/database

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|principal|string|false|read-only|Principal with the role on the table/database|
|role|string|false|read-only|Role associated with the principal|

<h2 id="tocS_GetAclPoliciesResponseBody">GetAclPoliciesResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemagetaclpoliciesresponsebody"></a>
<a id="schema_GetAclPoliciesResponseBody"></a>
<a id="tocSgetaclpoliciesresponsebody"></a>
<a id="tocsgetaclpoliciesresponsebody"></a>

```json
{
  "results": [
    {
      "principal": "string",
      "role": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|[[AclPolicy](#schemaaclpolicy)]|false|read-only|List of acl policies associated with table/database|

<h2 id="tocS_GetAllDatabasesResponseBody">GetAllDatabasesResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemagetalldatabasesresponsebody"></a>
<a id="schema_GetAllDatabasesResponseBody"></a>
<a id="tocSgetalldatabasesresponsebody"></a>
<a id="tocsgetalldatabasesresponsebody"></a>

```json
{
  "results": [
    {
      "databaseId": "my_database",
      "clusterId": "my_cluster"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|[[GetDatabaseResponseBody](#schemagetdatabaseresponsebody)]|false|read-only|List of Database objects|

<h2 id="tocS_GetDatabaseResponseBody">GetDatabaseResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemagetdatabaseresponsebody"></a>
<a id="schema_GetDatabaseResponseBody"></a>
<a id="tocSgetdatabaseresponsebody"></a>
<a id="tocsgetdatabaseresponsebody"></a>

```json
{
  "databaseId": "my_database",
  "clusterId": "my_cluster"
}

```

List of Database objects

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|databaseId|string|false|read-only|Unique Resource identifier for the Database|
|clusterId|string|false|read-only|Unique Resource identifier for the Cluster containing the Database|

