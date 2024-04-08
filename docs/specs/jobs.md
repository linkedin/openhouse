<!-- Generator: Widdershins v4.0.1 -->

<h1 id="openhouse-apis">OpenHouse APIs v0.0.1</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

API description for OpenHouse API

Base URLs:

* <a href="http://localhost:8080">http://localhost:8080</a>

<a href="http://swagger.io/terms">Terms of service</a>

License: <a href="http://springdoc.org">Apache 2.0</a>

<h1 id="openhouse-apis-job">Job</h1>

## Cancel Job

<a id="opIdcancelJob"></a>

> Code samples

`PUT /jobs/{jobId}/cancel`

Cancels the job given jobId

<h3 id="cancel-job-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|jobId|path|string|true|Job ID|

<h3 id="cancel-job-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Job PUT: UPDATED|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Job PUT: NOT_FOUND|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Job PUT: CONFLICT|None|

<aside class="success">
This operation does not require authentication
</aside>

## Submit a Job

<a id="opIdcreateJob"></a>

> Code samples

`POST /jobs`

Submits a Job and returns a Job resource.

> Body parameter

```json
{
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}"
}
```

<h3 id="submit-a-job-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateJobRequestBody](#schemacreatejobrequestbody)|true|none|

> Example responses

> 201 Response

```json
{
  "jobId": "my_job_8347adee-65b7-4e05-86fc-196af04f4e68",
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "state": "SUCCEEDED",
  "creationTimeMs": 1651002318265,
  "startTimeMs": 1651002318265,
  "finishTimeMs": 1651002318265,
  "lastUpdateTimeMs": 1651002318265,
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
  "executionId": "string"
}
```

<h3 id="submit-a-job-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Job POST: CREATED|[JobResponseBody](#schemajobresponsebody)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Job POST: BAD_REQUEST|[JobResponseBody](#schemajobresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

## Get Job

<a id="opIdgetJob"></a>

> Code samples

`GET /jobs/{jobId}`

Returns a Job resource identified by jobId.

<h3 id="get-job-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|jobId|path|string|true|Job ID|

> Example responses

> 200 Response

```json
{
  "jobId": "my_job_8347adee-65b7-4e05-86fc-196af04f4e68",
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "state": "SUCCEEDED",
  "creationTimeMs": 1651002318265,
  "startTimeMs": 1651002318265,
  "finishTimeMs": 1651002318265,
  "lastUpdateTimeMs": 1651002318265,
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
  "executionId": "string"
}
```

<h3 id="get-job-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Job GET: OK|[JobResponseBody](#schemajobresponsebody)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Job GET: NOT_FOUND|[JobResponseBody](#schemajobresponsebody)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_CreateJobRequestBody">CreateJobRequestBody</h2>
<!-- backwards compatibility -->
<a id="schemacreatejobrequestbody"></a>
<a id="schema_CreateJobRequestBody"></a>
<a id="tocScreatejobrequestbody"></a>
<a id="tocscreatejobrequestbody"></a>

```json
{
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}"
}

```

Request containing details of the Job to be created

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|jobName|string|true|none|Name of a job, doesn't need to be unique|
|clusterId|string|true|none|Unique identifier for the cluster|
|jobConf|[JobConf](#schemajobconf)|false|none|Job config|

<h2 id="tocS_JobConf">JobConf</h2>
<!-- backwards compatibility -->
<a id="schemajobconf"></a>
<a id="schema_JobConf"></a>
<a id="tocSjobconf"></a>
<a id="tocsjobconf"></a>

```json
"{'jobType': 'RETENTION', 'table': 'db.tb'}"

```

Job config

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|jobType|string|false|none|none|
|proxyUser|string|false|none|none|
|args|[string]|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|jobType|NO_OP|
|jobType|SQL_TEST|
|jobType|RETENTION|
|jobType|ORPHAN_FILES_DELETION|
|jobType|SNAPSHOTS_EXPIRATION|
|jobType|STAGED_FILES_DELETION|
|jobType|ORPHAN_DIRECTORY_DELETION| 

<h2 id="tocS_JobResponseBody">JobResponseBody</h2>
<!-- backwards compatibility -->
<a id="schemajobresponsebody"></a>
<a id="schema_JobResponseBody"></a>
<a id="tocSjobresponsebody"></a>
<a id="tocsjobresponsebody"></a>

```json
{
  "jobId": "my_job_8347adee-65b7-4e05-86fc-196af04f4e68",
  "jobName": "my_job",
  "clusterId": "my_cluster",
  "state": "SUCCEEDED",
  "creationTimeMs": 1651002318265,
  "startTimeMs": 1651002318265,
  "finishTimeMs": 1651002318265,
  "lastUpdateTimeMs": 1651002318265,
  "jobConf": "{'jobType': 'RETENTION', 'table': 'db.tb'}",
  "executionId": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|jobId|string|true|none|Unique auto-generated identifier for job prefixed with jobName|
|jobName|string|true|none|Name of a job, doesn't need to be unique|
|clusterId|string|true|none|Unique identifier for the cluster|
|state|string|true|none|Current job state, possible states: QUEUED, ACTIVE, CANCELLED, FAILED, SUCCEEDED|
|creationTimeMs|integer(int64)|true|none|Job creation time in unix epoch milliseconds|
|startTimeMs|integer(int64)|false|none|Job start time in unix epoch milliseconds|
|finishTimeMs|integer(int64)|false|none|Job finish time in unix epoch milliseconds|
|lastUpdateTimeMs|integer(int64)|false|none|Job contents last update time in unix epoch milliseconds|
|jobConf|[JobConf](#schemajobconf)|false|none|Job config|
|executionId|string|false|none|Execution ID generated from engine where job is submitted|

#### Enumerated Values

|Property|Value|
|---|---|
|state|QUEUED|
|state|RUNNING|
|state|CANCELLED|
|state|FAILED|
|state|SUCCEEDED|

