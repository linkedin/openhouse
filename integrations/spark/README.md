# OpenHouse Spark SQL extensions

## Unlocking tables

`ALTER TABLE ... UNLOCK` is disabled by default in Spark 3.1 and 3.5. Enable it for
the session with the boolean SQL configuration:

```sql
SET spark.sql.openhouse.unlock.enabled=true;
ALTER TABLE openhouse.my_database.my_table UNLOCK REASON SYSTEM_ONLY;
-- The form without REASON removes only a LEGACY lock.
ALTER TABLE openhouse.my_database.my_table UNLOCK;
SET spark.sql.openhouse.unlock.enabled=false;
```

Reasons are case-insensitive and validated while parsing. Only `LEGACY` and
`SYSTEM_ONLY` are accepted; unknown, empty or blank reasons fail before any
authorization or unlock request. An omitted reason becomes the explicit
`LEGACY` enum value, not null. The generated reason enum is carried through the
logical plan, execution and catalog API. Both omitted and explicit `LEGACY`
use the legacy DELETE endpoint; `SYSTEM_ONLY` uses the reason-targeted endpoint.
Direct catalog callers must supply a non-null reason enum.

Spark analysis resolves the catalog and database/table name once. An immutable
`UnlockTableTarget` is then reused for authorization and execution; changing the
session's current namespace after analysis cannot redirect the command.
Targets must have exactly one database component and a table name. Null, empty,
`.` and `..` components are rejected before any HTTP request, because a normalizing
proxy could interpret them as URL structure and change a DELETE's target.
Quoted names are preserved, not normalized or decoded again; the HTTP client
encodes each path component.

Invalid SQL targets raise a checked Spark `AnalysisException`. Direct Java
callers construct targets with `UnlockTableTarget.of(namespace, tableName)` and
handle the checked `UnlockValidationException`; passing a null reason to the
catalog also raises that exception. HTTP and transport failures retain the
catalog's existing `WebClientResponseWithMessageException` and
`WebClientRequestWithMessageException` conventions, including HTTP status codes.

Unlock request diagnostics use DEBUG, including the database/table identifier
and resolved reason. Successful requests emit a separate DEBUG success log;
failed requests do not.

The flag is a rollout control, not an authorization boundary. Spark analysis
checks the catalog's authenticated caller against the table service's existing
`LOCK_ADMIN` policy. It does not trust a Spark username or compare a
client-supplied owner name. Existing owner/metadata-updater and configured
lock-admin access is preserved where the service's authorization handler grants
that access; read permission alone is insufficient.

The check uses `HEAD /v1/databases/{databaseId}/tables/{tableId}/lock`, which returns
204 on authorization success without changing the table or lock. Neither the
check nor unlock requires an ordinary table load, so `SYSTEM_ONLY` locks do not
block the check. The DELETE endpoint rechecks permission before mutating the lock.
Authentication, authorization, unavailable-endpoint and transport errors fail
closed: Spark does not send the DELETE if the check fails.

Deploy the updated table service before enabling the command in Spark; older
services without the HEAD endpoint cannot authorize the command. Production
deployments must configure an enforcing authorization handler. The default OSS
OPA handler permits access when no OPA URI is configured.

`UnlockAuthorizationTest` exercises real Spark, HTTP authentication and the
embedded table service with an enforcing test policy for owners, metadata
updaters, lock admins and readers. Both Spark integration-test modules run it
through `catalogTest`; statement and transport tests cover the flag and
authorization failure paths.

## Command routing

Policy, column-tag and UNLOCK statements share one token-aware `ALTER TABLE`
dispatcher. Comments and quoted identifiers are handled by the lexer, so policy
keywords inside literals or names cannot capture ordinary Spark statements.
Other `ALTER TABLE` actions, including `RENAME TO unlock`, are delegated unchanged
to Spark.

Quoted identifiers follow Spark's escaping rules: remove the outer backticks and
decode doubled backticks to literal backticks exactly once. Removing every
backtick would change the identifier. This shared decoding applies to GRANT,
REVOKE, SHOW GRANTS, policy, column and UNLOCK commands; string literals are not
changed. Parser tests compare decoded names with Spark's native parser in both
supported versions.
