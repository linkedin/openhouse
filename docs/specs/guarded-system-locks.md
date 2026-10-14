<!-- Generator: Widdershins v4.0.1 -->

<h1 id="guarded-system_only-locks">Guarded SYSTEM_ONLY locks v0.1</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

API description for OpenHouse Tables API

<a href="http://swagger.io/terms">Terms of service</a>

License: <a href="http://springdoc.org">Apache 2.0</a>

# Authentication

- HTTP Authentication, scheme: bearer 

<h1 id="guarded-system_only-locks-table">Table</h1>

## Create a guarded SYSTEM_ONLY lock

<a id="opIdcreateGuardedSystemLockV1"></a>

> Code samples

`POST /v1/databases/{databaseId}/tables/{tableId}/lock/SYSTEM_ONLY/guarded`

Requires LOCK_ADMIN and the expected table UUID. An existing lock is a no-op only when its generation, authenticated owner and caller-selected lockId all match.

> Body parameter

```json
{
  "expectedTableUUID": "string",
  "lockId": "string",
  "message": "string"
}
```

<h3 id="create-a-guarded-system_only-lock-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|none|
|tableId|path|string|true|none|
|body|body|[CreateGuardedLockRequestBody](#schemacreateguardedlockrequestbody)|true|none|

<h3 id="create-a-guarded-system_only-lock-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Matching lock established|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Missing or invalid guards|None|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Lock administration denied|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table not found|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Generation, identity or metadata conflict|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

## Remove a guarded SYSTEM_ONLY lock

<a id="opIddeleteGuardedSystemLockV1"></a>

> Code samples

`DELETE /v1/databases/{databaseId}/tables/{tableId}/lock/SYSTEM_ONLY/guarded`

Requires LOCK_ADMIN, the expected table UUID and the recorded lockOwner/lockId. An inactive lock is a no-op only after checking the table generation.

<h3 id="remove-a-guarded-system_only-lock-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|databaseId|path|string|true|none|
|tableId|path|string|true|none|
|expectedTableUUID|query|string|true|none|
|lockOwner|query|string|true|none|
|lockId|query|string|true|none|

<h3 id="remove-a-guarded-system_only-lock-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Matching lock removed or already inactive|None|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Missing or invalid guards|None|
|403|[Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)|Lock administration denied|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Table not found|None|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Generation, identity or metadata conflict|None|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
BearerAuth
</aside>

# Schemas

<h2 id="tocS_CreateGuardedLockRequestBody">CreateGuardedLockRequestBody</h2>
<!-- backwards compatibility -->
<a id="schemacreateguardedlockrequestbody"></a>
<a id="schema_CreateGuardedLockRequestBody"></a>
<a id="tocScreateguardedlockrequestbody"></a>
<a id="tocscreateguardedlockrequestbody"></a>

```json
{
  "expectedTableUUID": "string",
  "lockId": "string",
  "message": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|expectedTableUUID|string|true|none|Expected table UUID from GET table.|
|lockId|string|true|none|Caller-selected lock identity, stable across retries and distinct from other workflows.|
|message|string|false|none|Human-readable lock description.|

<h2 id="tocS_LockState">LockState</h2>
<!-- backwards compatibility -->
<a id="schemalockstate"></a>
<a id="schema_LockState"></a>
<a id="tocSlockstate"></a>
<a id="tocslockstate"></a>

```json
"{lockState: {locked: true/false}"

```

Locking state of table.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|locked|boolean|false|none|locked state of a table, example = false, true|
|message|string|false|none|message for locking|
|reason|string¦null|false|none|Omitted or null reasons are interpreted as LEGACY when locked=true; otherwise they remain null. Explicit reasons are preserved.|
|lockOwner|string¦null|false|read-only|Authenticated principal that created a guarded lock.|
|tableUUID|string¦null|false|read-only|Table generation pinned by a guarded lock.|
|lockId|string¦null|false|read-only|Caller-selected identity of a guarded lock.|
|creationTime|integer(int64)|false|none|lock creation epoch time measured in UTC milliseconds for a table|
|expirationInDays|integer(int32)|false|none|lock expiration time for a table is `n` days from creationTime|

#### Enumerated Values

|Property|Value|
|---|---|
|reason|LEGACY|
|reason|SYSTEM_ONLY|

