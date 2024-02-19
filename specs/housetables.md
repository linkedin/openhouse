<!-- Generator: Widdershins v4.0.1 -->

<h1 id="house-tables-api">House Tables API v0.0.1</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

API description for House Tables API

Base URLs:

* <a href="http://localhost:8080">http://localhost:8080</a>

<a href="http://swagger.io/terms">Terms of service</a>

License: <a href="http://springdoc.org">Apache 2.0</a>

<h1 id="house-tables-api-usertable">UserTable</h1>

## Get User Table identified by databaseID and tableId.

<a id="opIdgetUserTable"></a>

> Code samples

`GET /hts/tables`

Returns a User House Table identified by databaseID and tableId.

<h3 id="get-user-table-identified-by-databaseid-and-tableid.-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|query|string|true|none|
|tableId|query|string|true|none|

> Example responses

> 200 Response

```json
{
  "entity": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "tableVersion": "string",
    "metadataLocation": "string"
  }
}
```

<h3 id="get-user-table-identified-by-databaseid-and-tableid.-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|User Table GET: OK|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|User Table GET: TBL_DB_NOT_FOUND|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|

<aside class="success">
This operation does not require authentication
</aside>

## Update a User Table

<a id="opIdputUserTable"></a>

> Code samples

`PUT /hts/tables`

Updates or creates a User House Table identified by databaseID and tableId. If the table does not exist, it will be created. If the table exists, it will be updated.

> Body parameter

```json
{
  "entity": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "tableVersion": "string",
    "metadataLocation": "string"
  }
}
```

<h3 id="update-a-user-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateUpdateEntityRequestBodyUserTable](#schemacreateupdateentityrequestbodyusertable)|true|none|

> Example responses

> 200 Response

```json
{
  "entity": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "tableVersion": "string",
    "metadataLocation": "string"
  }
}
```

<h3 id="update-a-user-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|User Table PUT: UPDATED|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|User Table PUT: CREATED|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|User Table PUT: BAD_REQUEST|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|User Table PUT: DB_NOT_FOUND|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|User Table PUT: CONFLICT|[EntityResponseBodyUserTable](#schemaentityresponsebodyusertable)|

<aside class="success">
This operation does not require authentication
</aside>

## Delete a User Table

<a id="opIddeleteTable"></a>

> Code samples

`DELETE /hts/tables`

Delete a User House Table entry identified by databaseID and tableId.

<h3 id="delete-a-user-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|query|string|true|none|
|tableId|query|string|true|none|

<h3 id="delete-a-user-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|User Table DELETE: NO_CONTENT|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|User Table DELETE: BAD_REQUEST|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|User Table DELETE: TBL_DB_NOT_FOUND|None|

<aside class="success">
This operation does not require authentication
</aside>

## Search User Table by filter.

<a id="opIdgetUserTables"></a>

> Code samples

`GET /hts/tables/query`

Returns user table from house table that fulfills the predicate. For examples, one could provide {databaseId: d1} in the map to query all tables from database d1.

<h3 id="search-user-table-by-filter.-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|parameters|query|object|true|none|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "tableId": "my_table",
      "databaseId": "my_database",
      "tableVersion": "string",
      "metadataLocation": "string"
    }
  ]
}
```

<h3 id="search-user-table-by-filter.-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|User Table GET: OK|[GetAllEntityResponseBodyUserTable](#schemagetallentityresponsebodyusertable)|

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="house-tables-api-job">Job</h1>

## Get job details for a given jobId.

<a id="opIdgetJob"></a>

> Code samples

`GET /hts/jobs`

Returns a Job entity identified by jobId

<h3 id="get-job-details-for-a-given-jobid.-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|jobId|query|string|true|none|

> Example responses

> 200 Response

```json
{
  "entity": {
    "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
    "state": "QUEUED",
    "version": "539482",
    "jobName": "my_job",
    "clusterId": "my_cluster",
    "creationTimeMs": 1651002318265,
    "startTimeMs": 1651002318265,
    "finishTimeMs": 1651002318265,
    "lastUpdateTimeMs": 1651002318265,
    "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
    "heartbeatTimeMs": 1651002318265,
    "executionId": "application_1642969576960_13278206"
  }
}
```

<h3 id="get-job-details-for-a-given-jobid.-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Job GET: OK|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Job GET: NOT_FOUND|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|

<aside class="success">
This operation does not require authentication
</aside>

## Update a Job

<a id="opIdputJob"></a>

> Code samples

`PUT /hts/jobs`

Updates or creates a Job in the House Table and returns the job resources. If the job does not exist, it will be created. If the job exists, it will be updated.

> Body parameter

```json
{
  "entity": {
    "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
    "state": "QUEUED",
    "version": "539482",
    "jobName": "my_job",
    "clusterId": "my_cluster",
    "creationTimeMs": 1651002318265,
    "startTimeMs": 1651002318265,
    "finishTimeMs": 1651002318265,
    "lastUpdateTimeMs": 1651002318265,
    "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
    "heartbeatTimeMs": 1651002318265,
    "executionId": "application_1642969576960_13278206"
  }
}
```

<h3 id="update-a-job-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateUpdateEntityRequestBodyJob](#schemacreateupdateentityrequestbodyjob)|true|none|

> Example responses

> 200 Response

```json
{
  "entity": {
    "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
    "state": "QUEUED",
    "version": "539482",
    "jobName": "my_job",
    "clusterId": "my_cluster",
    "creationTimeMs": 1651002318265,
    "startTimeMs": 1651002318265,
    "finishTimeMs": 1651002318265,
    "lastUpdateTimeMs": 1651002318265,
    "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
    "heartbeatTimeMs": 1651002318265,
    "executionId": "application_1642969576960_13278206"
  }
}
```

<h3 id="update-a-job-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Job PUT: UPDATED|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Job PUT: CREATED|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Job PUT: BAD_REQUEST|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Job PUT: DB_NOT_FOUND|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Job PUT: CONFLICT|[EntityResponseBodyJob](#schemaentityresponsebodyjob)|

<aside class="success">
This operation does not require authentication
</aside>

## Delete a Job

<a id="opIddeleteJob"></a>

> Code samples

`DELETE /hts/jobs`

Delete a Job entity identified by jobId

<h3 id="delete-a-job-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|jobId|query|string|true|none|

<h3 id="delete-a-job-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Job DELETE: NO_CONTENT|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Job DELETE: BAD_REQUEST|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Job DELETE: NOT_FOUND|None|

<aside class="success">
This operation does not require authentication
</aside>

## Search Jobs by filter

<a id="opIdgetAllJobs"></a>

> Code samples

`GET /hts/jobs/query`

Returns jobs that fulfills the filter predicate. For examples, one could provide {status: <QUEUED>} in the map to query all jobs with that status. 

<h3 id="search-jobs-by-filter-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|parameters|query|object|true|none|

> Example responses

> 200 Response

```json
{
  "results": [
    {
      "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
      "state": "QUEUED",
      "version": "539482",
      "jobName": "my_job",
      "clusterId": "my_cluster",
      "creationTimeMs": 1651002318265,
      "startTimeMs": 1651002318265,
      "finishTimeMs": 1651002318265,
      "lastUpdateTimeMs": 1651002318265,
      "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
      "heartbeatTimeMs": 1651002318265,
      "executionId": "application_1642969576960_13278206"
    }
  ]
}
```

<h3 id="search-jobs-by-filter-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Job GET: OK|[GetAllEntityResponseBodyJob](#schemagetallentityresponsebodyjob)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_CreateUpdateEntityRequestBodyUserTable">CreateUpdateEntityRequestBodyUserTable</h2>
<!-- backwards compatibility -->
<a id="schemacreateupdateentityrequestbodyusertable"></a>
<a id="schema_CreateUpdateEntityRequestBodyUserTable"></a>
<a id="tocScreateupdateentityrequestbodyusertable"></a>
<a id="tocscreateupdateentityrequestbodyusertable"></a>

```json
{
  "entity": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "tableVersion": "string",
    "metadataLocation": "string"
  }
}

```

Request containing details of the User Table to be created/updated

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|entity|[UserTable](#schemausertable)|true|none|The entity object that clients want to create/update in the target house table.|

<h2 id="tocS_UserTable">UserTable</h2>
<!-- backwards compatibility -->
<a id="schemausertable"></a>
<a id="schema_UserTable"></a>
<a id="tocSusertable"></a>
<a id="tocsusertable"></a>

```json
{
  "tableId": "my_table",
  "databaseId": "my_database",
  "tableVersion": "string",
  "metadataLocation": "string"
}

```

The entity object that clients want to create/update in the target house table.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|tableId|string|true|none|Unique Resource identifier for a table within a Database.|
|databaseId|string|true|none|Unique Resource identifier for the Database containing the Table. Together with tableID they form a composite primary key for a user table.|
|tableVersion|string|false|none|Current Version of the user table.|
|metadataLocation|string|true|none|Full URI for the file manifesting the newest version of a user table.|

<h2 id="tocS_EntityResponseBodyUserTable">EntityResponseBodyUserTable</h2>
<!-- backwards compatibility -->
<a id="schemaentityresponsebodyusertable"></a>
<a id="schema_EntityResponseBodyUserTable"></a>
<a id="tocSentityresponsebodyusertable"></a>
<a id="tocsentityresponsebodyusertable"></a>

```json
{
  "entity": {
    "tableId": "my_table",
    "databaseId": "my_database",
    "tableVersion": "string",
    "metadataLocation": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|entity|[UserTable](#schemausertable)|true|none|The entity object that clients want to create/update in the target house table.|

<h2 id="tocS_CreateUpdateEntityRequestBodyJob">CreateUpdateEntityRequestBodyJob</h2>
<!-- backwards compatibility -->
<a id="schemacreateupdateentityrequestbodyjob"></a>
<a id="schema_CreateUpdateEntityRequestBodyJob"></a>
<a id="tocScreateupdateentityrequestbodyjob"></a>
<a id="tocscreateupdateentityrequestbodyjob"></a>

```json
{
  "entity": {
    "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
    "state": "QUEUED",
    "version": "539482",
    "jobName": "my_job",
    "clusterId": "my_cluster",
    "creationTimeMs": 1651002318265,
    "startTimeMs": 1651002318265,
    "finishTimeMs": 1651002318265,
    "lastUpdateTimeMs": 1651002318265,
    "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
    "heartbeatTimeMs": 1651002318265,
    "executionId": "application_1642969576960_13278206"
  }
}

```

Request containing details of the User Table to be created/updated

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|entity|[Job](#schemajob)|true|none|The entity object that clients want to create/update in the target house table.|

<h2 id="tocS_Job">Job</h2>
<!-- backwards compatibility -->
<a id="schemajob"></a>
<a id="schema_Job"></a>
<a id="tocSjob"></a>
<a id="tocsjob"></a>

```json
{
  "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
  "state": "QUEUED",
  "version": "539482",
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "creationTimeMs": 1651002318265,
  "startTimeMs": 1651002318265,
  "finishTimeMs": 1651002318265,
  "lastUpdateTimeMs": 1651002318265,
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
  "heartbeatTimeMs": 1651002318265,
  "executionId": "application_1642969576960_13278206"
}

```

The entity object that clients want to create/update in the target house table.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|jobId|string|true|none|Unique Resource identifier for a job within a Database.|
|state|string|false|none|State for the job|
|version|string|false|none|Version of the job entry in HTS. HTS internally generates the next version after a successful PUT.The value would be a string representing a random integer.|
|jobName|string|true|none|Name of a job, doesn't need to be unique|
|clusterId|string|true|none|Unique identifier for the cluster|
|creationTimeMs|integer(int64)|false|none|Job creation time in unix epoch milliseconds|
|startTimeMs|integer(int64)|false|none|Job start time in unix epoch milliseconds|
|finishTimeMs|integer(int64)|false|none|Job finish time in unix epoch milliseconds|
|lastUpdateTimeMs|integer(int64)|false|none|Job contents last update time in unix epoch milliseconds|
|jobConf|string|false|none|Job config|
|heartbeatTimeMs|integer(int64)|false|none|Running job heartbeat timestamp in milliseconds|
|executionId|string|false|none|Launched job execution id specific to engine|

<h2 id="tocS_EntityResponseBodyJob">EntityResponseBodyJob</h2>
<!-- backwards compatibility -->
<a id="schemaentityresponsebodyjob"></a>
<a id="schema_EntityResponseBodyJob"></a>
<a id="tocSentityresponsebodyjob"></a>
<a id="tocsentityresponsebodyjob"></a>

```json
{
  "entity": {
    "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
    "state": "QUEUED",
    "version": "539482",
    "jobName": "my_job",
    "clusterId": "my_cluster",
    "creationTimeMs": 1651002318265,
    "startTimeMs": 1651002318265,
    "finishTimeMs": 1651002318265,
    "lastUpdateTimeMs": 1651002318265,
    "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
    "heartbeatTimeMs": 1651002318265,
    "executionId": "application_1642969576960_13278206"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|entity|[Job](#schemajob)|true|none|The entity object that clients want to create/update in the target house table.|

<h2 id="tocS_GetAllEntityResponseBodyUserTable">GetAllEntityResponseBodyUserTable</h2>
<!-- backwards compatibility -->
<a id="schemagetallentityresponsebodyusertable"></a>
<a id="schema_GetAllEntityResponseBodyUserTable"></a>
<a id="tocSgetallentityresponsebodyusertable"></a>
<a id="tocsgetallentityresponsebodyusertable"></a>

```json
{
  "results": [
    {
      "tableId": "my_table",
      "databaseId": "my_database",
      "tableVersion": "string",
      "metadataLocation": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|[[UserTable](#schemausertable)]|false|read-only|List of user table objects in House table|

<h2 id="tocS_GetAllEntityResponseBodyJob">GetAllEntityResponseBodyJob</h2>
<!-- backwards compatibility -->
<a id="schemagetallentityresponsebodyjob"></a>
<a id="schema_GetAllEntityResponseBodyJob"></a>
<a id="tocSgetallentityresponsebodyjob"></a>
<a id="tocsgetallentityresponsebodyjob"></a>

```json
{
  "results": [
    {
      "jobId": "24efc962-9962-4522-b0b6-29490d7d8a0e",
      "state": "QUEUED",
      "version": "539482",
      "jobName": "my_job",
      "clusterId": "my_cluster",
      "creationTimeMs": 1651002318265,
      "startTimeMs": 1651002318265,
      "finishTimeMs": 1651002318265,
      "lastUpdateTimeMs": 1651002318265,
      "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
      "heartbeatTimeMs": 1651002318265,
      "executionId": "application_1642969576960_13278206"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|[[Job](#schemajob)]|false|read-only|List of user table objects in House table|

