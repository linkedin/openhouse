# Table lock lifecycle REST API

Lock administration uses the existing `LOCK_ADMIN` authorization. It does not
require `SYSTEM_ADMIN`; replica lock/unlock operations remain unsupported.
Status uses `GET_TABLE_METADATA` authorization, independently of the normal
table-read path. Authenticate all requests as usual:

```sh
TABLE_URL="$OPENHOUSE_URL/v1/databases/$DATABASE_ID/tables/$TABLE_ID"
curl --fail-with-body -H "Authorization: Bearer $TOKEN" "$TABLE_URL/lock"
```

The status response contains only the current `tableUUID` and nullable
`lockState`, not schema, table properties, or data/metadata locations. An
unlocked table returns HTTP 200 with `"lockState": null`; a missing table returns
404 and denied metadata access returns 403. An active lock includes its reason,
message, creation time, expiration, and any recorded owner/generation. Omitted
or null reasons in older locks still read as `LEGACY`.

## Read/write evaluation

All data access still requires its normal authorization. The declaration
`X-OpenHouse-System-Action: true` permits otherwise-authorized access to an
active `TIER3_AUTO_CLEANUP` lock; it is not an ACL permission or an administrator
bypass. Even the table owner is blocked without the declaration.

| Active lock | System-action declaration | Table reads | Table/snapshot writes |
| --- | --- | --- | --- |
| None | Any/absent | Existing ACL rules | Existing ACL rules |
| `LEGACY` | False/absent or true | Existing `LOCK_ADMIN` and metadata ACL checks | Rejected |
| `TIER3_AUTO_CLEANUP` | False/absent | Rejected | Rejected |
| `TIER3_AUTO_CLEANUP` | True | Allowed with metadata ACL | Allowed with write ACL |

The values `true` and `false` are case-insensitive. An absent declaration,
including calls outside a Servlet request, defaults to false. Other supplied
values, including blank values or surrounding whitespace, return 400 when
cleanup data access is evaluated. The header is not evaluated for unlocked
tables, legacy locks, or control operations.

The write rule includes ordinary metadata updates, snapshot updates, staged
replacement, snapshot replace commits/RTAS, and rename. Permitted writes still
cannot alter cleanup lock identity through a policy payload. Authorization is
checked before returning cleanup-denial details. A denial includes the reason,
table identifier, any descriptive lock message, and guidance to promote the
table to Tier 2 to retain it or use authorized reason-targeted unlock. Message
text and client names never select an exception to these rules.

For example, an authorized system operation can read the table with:

```sh
curl --fail-with-body "$TABLE_URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-OpenHouse-System-Action: true"
```

Status and authorized unlock remain usable without this header. The declaration
does not bypass unlock identity guards, locked-table grant restrictions, or any
control/DROP authorization. This is **not** a generation-guarded DROP API;
coordinated DLM/server deletion remains separate work.

## Create a cleanup lock

Read status first and use that response's current UUID:

```sh
curl --fail-with-body -X POST "$TABLE_URL/lock" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  --data '{
    "locked": true,
    "reason": "TIER3_AUTO_CLEANUP",
    "expectedTableUUID": "<current tableUUID>",
    "message": "Tier 3 automatic cleanup"
  }'
```

The server records the acting principal as `lockOwner` and the current table
UUID as the lock's `tableUUID`. A caller cannot select another lock owner.
Missing/blank `expectedTableUUID` is 400; a stale generation is 409.

There is at most one active lock. If either the requested or existing active
lock is a cleanup lock, a different reason, owner, or generation is rejected
with 409. A retry with the same reason, owner, and generation succeeds with
201 without rewriting metadata, message, creation time, or expiration.
Legacy-to-legacy updates retain their existing behavior and require no UUID.

## Guarded cleanup unlock

Use the current UUID and recorded owner from the status response, not the
principal of the administrator performing recovery:

```sh
curl --fail-with-body -X DELETE -G \
  "$TABLE_URL/lock/TIER3_AUTO_CLEANUP" \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "expectedTableUUID=$TABLE_UUID" \
  --data-urlencode "expectedLockOwner=$LOCK_OWNER"
```

Any authorized lock administrator can perform this operation. The server
checks the current table generation, active lock reason, stored generation,
and stored owner before removing the lock. Missing/blank guards or an invalid
reason are 400. Mismatches are 409, without changing the lock. Success is 204.
An already-inactive lock also returns 204, but only after validating the
guards and current table generation: retrying against a recreated table is
not silently accepted.

### Recover a pre-owner cleanup lock

Early cleanup locks could be stored without **both** `lockOwner` and
`tableUUID`. They remain cleanup locks, not legacy locks. After inspecting
status and confirming both fields are absent/null, an authorized administrator
can explicitly use:

```sh
curl --fail-with-body -X DELETE -G \
  "$TABLE_URL/lock/TIER3_AUTO_CLEANUP" \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "expectedTableUUID=$TABLE_UUID" \
  --data-urlencode "expectedLockOwner=__UNRECORDED__"
```

This compatibility path still requires the current UUID, cleanup reason, and
`LOCK_ADMIN`. It does not bypass owner/generation checks when either recorded
field is present, nor can a create retry claim an unowned active lock.

## Legacy compatibility and scope

`DELETE /v1/databases/{databaseId}/tables/{tableId}/lock` removes **only LEGACY**
locks and rejects cleanup locks with 409. The generated Java method remains
`deleteLockV1(String databaseId, String tableId)`. New SDK methods are
`getLockV1(databaseId, tableId)` and
`deleteLockByReasonV1(databaseId, tableId, reason, expectedTableUUID, expectedLockOwner)`.

General table and snapshot writes cannot introduce or alter cleanup lock
metadata, including staged create/replace and replace-commit requests.
Omitting policies or their lock state does not erase existing cleanup lock
metadata; omitting the entire policy object also carries forward unrelated
policies. Use the lifecycle endpoints to change a cleanup lock.

The current implementation includes lifecycle guards and cleanup read/write
evaluation. Request auditing, production job propagation, automated deletion,
generation-guarded DROP/DLM integration, and SQL unlock remain separate work.
