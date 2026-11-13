# Namespace metadata administration

Namespace metadata stores optional defaults for consumers that explicitly request effective
table properties. Ordinary table operations remain independent of these records. Explicit
table properties take precedence over defaults.

Administrators manage defaults through the public namespace API. Creating, updating, and
dropping metadata require SYSTEM_ADMIN; metadata reads use GET_TABLE_METADATA. The internal
House Tables API persists the metadata and is intended for service callers.

## Names and property changes

Names resolve case-insensitively and retain their insertion spelling. Creating `Finance`
and then requesting `FINANCE` returns the same identity and the name `Finance`. A duplicate
create with another spelling fails without replacing the existing record.

Updates set the supplied keys and remove the requested keys while retaining all other
properties. Property names are case-sensitive. A namespace keeps its UUID across updates;
dropping and recreating it creates a new UUID. Concurrent edits can conflict, including
an empty update. After a conflict, read the current properties and decide whether the intended
edit still applies before submitting it again.

Create, update, and removal keys must contain well-formed Unicode and at most 512 UTF-16 code
units. Empty keys, spaces, accents, case differences, and supplementary characters round-trip.

## Deletion and recovery

Deleting namespace metadata leaves table metadata and files intact. The public API rejects
drop while a table or view occupies the namespace. Table creation and metadata deletion are
independent, so a concurrent table create can succeed even when a namespace drop succeeds.
A namespace record is not a mandatory parent of a table.

A lost response does not establish whether a mutation committed. Read the metadata and
reconcile its identity and properties with the intended change before retrying. In particular,
a repeated create can encounter an existing record and a repeated delete can encounter an
absent record after the original request succeeded.
