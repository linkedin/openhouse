"""Deployment-level end-to-end tests for the HouseTables Service (HTS) REST API.

Unlike the in-process ``@SpringBootTest`` suites under
``services/housetables/src/test/java/.../e2e``, this script talks real HTTP to a
running HTS deployment, so it exercises JSON serialization, query-parameter
binding and HTTP status codes as a client actually sees them.

HTS is unauthenticated: there is no token argument and a request that omits a
required parameter returns 400, never 401.

CI runs against MySQL in Docker, initialized from services/housetables/ddl.
Each test seeds rows under a database id unique to the run and deletes them
afterward, keeping the suite re-runnable against a warm deployment. API-only
cases can also run against an H2-backed HTS deployment.

NOT EVERY CASE IS REACHABLE OVER HTTP. A legacy row whose
``user_table_row.entity_type`` column is NULL must resolve as a TABLE, but no
table/view PUT can create one: ``HtsEntityTypeValidator.normalize`` stamps the
route's non-null discriminator at ingress, and the JPA converter rejects null
writes. Restore supplies TABLE through ``UserTablesMapper`` instead.
Those cases connect to the database directly, plant the row, and then assert
through the deployed API. They also close the one thing an API read cannot
prove: ``EntityTypeConverter`` resolves a stored NULL to TABLE on read, so a
response saying "TABLE" is equally consistent with a stored ``'TABLE'`` and a
stored NULL. Only a column read tells those apart.

Whether that is possible depends on how the deployment is configured:

* ``database.type: IN_MEMORY`` -- H2 inside the HTS JVM heap. The direct-database
  helpers cannot connect to it.
* ``database.type: MYSQL`` -- the direct-database helpers can connect when
  PyMySQL and matching connection settings are available.

Connection settings come from ``HTS_DB_HOST``, ``HTS_DB_PORT``, ``HTS_DB_USER``,
``HTS_DB_PASSWORD`` and ``HTS_DB_NAME``, defaulting to the local docker-compose
MySQL recipe. The helpers do not detect the service's database type: settings
must point to that same service's MySQL store, not an unrelated database.
Database-backed cases skip when PyMySQL is absent or connection setup fails;
the final summary reports those skips separately. CI configures MySQL access
so these cases can run rather than skip.

Worth knowing when reading the restore cases: ``soft_deleted_user_table_row``
has no ``entity_type`` column at all, so the discriminator is genuinely
destroyed on soft delete, and ``UserTablesServiceImpl.restoreUserTable``
rebuilds a live row from that column-less source. ``UserTablesMapper`` supplies
TABLE, and the converter rejects a missing stamp at write time::

    @Mapping(target = "entityType", expression = "java(EntityType.TABLE)")
    UserTableRow toUserTableRow(SoftDeletedUserTableRow softDeletedUserTableRow);
"""

import os
import sys
import time
import uuid

import requests

try:
    import pymysql
except ImportError:  # pragma: no cover - exercised only where the dep is absent
    pymysql = None

DEFAULT_HOST = 'http://localhost:8001'
INITIAL_VERSION = 'INITIAL_VERSION'
JSON_HEADERS = {'Content-Type': 'application/json'}

DB_SETTINGS = {
    'host': os.environ.get('HTS_DB_HOST', '127.0.0.1'),
    'port': int(os.environ.get('HTS_DB_PORT', '3306')),
    'user': os.environ.get('HTS_DB_USER', 'oh_user'),
    'password': os.environ.get('HTS_DB_PASSWORD', 'oh_password'),
    'database': os.environ.get('HTS_DB_NAME', 'oh_db'),
}

# Unique per run so that repeated runs against a warm container never collide.
RUN_ID = f'{int(time.time())}_{uuid.uuid4().hex[:8]}'

HOST = DEFAULT_HOST


class SkippedTest(Exception):
    """Raised by a test that cannot run in this deployment's configuration."""


def database_connection():
    """    A MySQL connection using HTS_DB_*, or None if the driver/connect step fails.

    This does not discover or verify the service's backing store. The caller
    must provide the matching MySQL settings; an embedded H2 store cannot be
    reached through this helper.

    SET NAMES pins utf8mb4_0900_ai_ci (NO PAD), not merely a character set.
    For example, ``SELECT 'TABLE ' = 'TABLE'`` is false with that collation
    but true with a PAD SPACE collation such as latin1_swedish_ci. Comparisons
    against the entity_type column use that column's collation, which the
    corruption test verifies separately.
    """
    if pymysql is None:
        return None
    try:
        connection = pymysql.connect(
            connect_timeout=5, autocommit=True, charset='utf8mb4', **DB_SETTINGS)
    except Exception:  # pymysql raises several unrelated types on an absent server
        return None
    with connection.cursor() as cursor:
        cursor.execute('SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci')
    return connection


def require_database():
    connection = database_connection()
    if connection is None:
        raise SkippedTest(
            f"no database connection to "
            f"{DB_SETTINGS['user']}@{DB_SETTINGS['host']}:{DB_SETTINGS['port']}"
            f"/{DB_SETTINGS['database']} "
            f"(expected for an IN_MEMORY deployment; override with HTS_DB_* env vars)")
    return connection


def query_one(connection, sql: str, parameters: tuple):
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchone()


def execute(connection, sql: str, parameters: tuple) -> int:
    with connection.cursor() as cursor:
        return cursor.execute(sql, parameters)


def read_entity_type_column(connection, database: str, table: str) -> tuple:
    """The raw discriminator plus whether it is SQL NULL, which a read cannot infer."""
    return query_one(
        connection,
        'SELECT entity_type, entity_type IS NULL FROM user_table_row '
        'WHERE database_id = %s AND table_id = %s',
        (database, table))


def count_rows(connection, database: str, table: str) -> int:
    return query_one(
        connection,
        'SELECT COUNT(*) FROM user_table_row WHERE database_id = %s AND table_id = %s',
        (database, table))[0]


def plant_entity_type(connection, database: str, table: str, entity_type,
                      metadata_location: str = '/tmp/legacy.json') -> None:
    """Insert a row carrying an arbitrary discriminator, including NULL.

    Table/view PUTs cannot produce these: ``HtsEntityTypeValidator.normalize``
    stamps a valid discriminator, and the JPA converter rejects null writes.
    """
    execute(
        connection,
        'INSERT INTO user_table_row '
        '(database_id, table_id, version, metadata_location, storage_type, '
        'creation_time, entity_type) '
        "VALUES (%s, %s, 0, %s, 'hdfs', NULL, %s)",
        (database, table, metadata_location, entity_type))


def plant_legacy_null_row(connection, database: str, table: str,
                          metadata_location: str = '/tmp/legacy.json') -> None:
    """Insert a row with a NULL entity_type, which no HTTP request can produce."""
    plant_entity_type(connection, database, table, None, metadata_location)


def purge_database_rows(connection, database: str) -> None:
    execute(connection, 'DELETE FROM user_table_row WHERE database_id = %s', (database,))
    execute(connection,
            'DELETE FROM soft_deleted_user_table_row WHERE database_id = %s', (database,))


def database_id(suffix: str) -> str:
    """A database id scoped to this run, so tests never see each other's rows."""
    return f'hts_it_{RUN_ID}_{suffix}'


def describe(response: requests.Response) -> str:
    """Everything needed to diagnose a failed assertion."""
    return (f"{response.request.method} {response.request.url} -> "
            f"{response.status_code} {response.text}")


def assert_status(response: requests.Response, expected: int, what: str) -> None:
    assert response.status_code == expected, \
        f"{what}: expected HTTP {expected}, got {describe(response)}"


def assert_message(response: requests.Response, expected: str, what: str) -> None:
    actual = response.json().get('message')
    assert actual == expected, \
        f"{what}: expected message {expected!r}, got {actual!r}. {describe(response)}"


def assert_message_contains(response: requests.Response, fragments, what: str) -> None:
    """Assert the response message carries every one of ``fragments``.

    Deliberately not equality. The fragments passed in are the parts of a
    diagnostic that carry meaning, so rewording the prose around them does not
    break the assertion, while a message that stops naming them still does.
    """
    actual = response.json().get('message') or ''
    for fragment in fragments:
        assert fragment in actual, \
            f"{what}: expected the message to contain {fragment!r}, got {actual!r}. " \
            f"{describe(response)}"


def assert_corrupt_value_diagnostic(response: requests.Response, stored: str,
                                    what: str) -> None:
    """The converter's 500 must name the column and quote the offending value.

    The quoting is asserted rather than treated as incidental formatting. An
    unquoted rendering collapses ``''`` to ``[]`` and ``'TABLE '`` to
    ``[TABLE ]``, which is exactly the information an operator needs and exactly
    what is impossible to read without the quotes.
    """
    assert_message_contains(
        response, ('user_table_row.entity_type', f"'{stored}'"), what)


def put_entity(kind: str, database: str, table: str, table_version: str = INITIAL_VERSION,
               metadata_location: str = None, entity_type: str = None) -> requests.Response:
    """PUT /hts/tables or PUT /hts/views.

    ``table_version`` is a compare-and-swap token, not a counter: INITIAL_VERSION
    creates, and the row's current ``metadataLocation`` updates. Anything else is
    a 409.
    """
    entity = {
        'databaseId': database,
        'tableId': table,
        'tableVersion': table_version,
        'metadataLocation': metadata_location or f'/tmp/{database}/{table}',
        'storageType': 'hdfs',
    }
    if entity_type is not None:
        entity['entityType'] = entity_type
    return requests.put(f'{HOST}/hts/{kind}', json={'entity': entity}, headers=JSON_HEADERS)


def create_table(database: str, table: str, entity_type: str = None) -> dict:
    response = put_entity('tables', database, table, entity_type=entity_type)
    assert_status(response, 201, f"seeding table {database}.{table}")
    return response.json()['entity']


def create_view(database: str, table: str, entity_type: str = None) -> dict:
    response = put_entity('views', database, table, entity_type=entity_type)
    assert_status(response, 201, f"seeding view {database}.{table}")
    return response.json()['entity']


def get_entity(kind: str, database: str, table: str) -> requests.Response:
    """GET /hts/tables, /hts/views or /hts/entities for a single key."""
    return requests.get(f'{HOST}/hts/{kind}', params={'databaseId': database, 'tableId': table})


EXISTS = 'exists'
ABSENT = 'absent'
ERROR = 'error'


def existence_state(response: requests.Response) -> str:
    """What one endpoint claims about a key: 200 exists, 404 absent, else error.

    A 500 is neither ``EXISTS`` nor ``ABSENT``, so it gets its own state rather
    than being folded into either. Folding would launder a broken row into a
    confident answer and defeat the comparison this feeds: map 500 to ``ABSENT``
    and a 404/500 row reads as agreement, which is precisely the disagreement
    worth catching; map it to ``EXISTS`` and a 200/500 row does the same. Kept
    distinct, two errors agree only at the error-state level. That alone does
    not establish a shared cause; the corruption tests separately check the
    converter diagnostic.
    """
    if response.status_code == 200:
        return EXISTS
    if response.status_code == 404:
        return ABSENT
    return ERROR


def endpoints_agree_on_existence(database: str, table: str, kind: str = 'tables') -> tuple:
    """Read one key through both the neutral and a typed route and compare verdicts.

    ``kind`` must be the typed route matching the row's own type. A view read
    through /hts/tables is a 404 by design -- that is type scoping working, not
    a disagreement -- so comparing a view against the table route would measure
    the wrong thing.

    Returns ``(agree, neutral_state, typed_state, neutral_response,
    typed_response)`` so a caller can assert agreement or, for a known-bad row,
    assert the specific disagreement.
    """
    neutral = get_entity('entities', database, table)
    typed = get_entity(kind, database, table)
    neutral_state = existence_state(neutral)
    typed_state = existence_state(typed)
    return neutral_state == typed_state, neutral_state, typed_state, neutral, typed


def assert_endpoints_agree(database: str, table: str, kind: str, what: str) -> None:
    agree, neutral_state, typed_state, neutral, typed = endpoints_agree_on_existence(
        database, table, kind)
    assert agree, \
        f"{what}: /hts/entities says {neutral_state} but /hts/{kind} says " \
        f"{typed_state} for the same key. {describe(neutral)} || {describe(typed)}"


def delete_entity(kind: str, database: str, table: str) -> requests.Response:
    return requests.delete(f'{HOST}/hts/{kind}', params={'databaseId': database, 'tableId': table})


def query_entities(kind: str, database: str) -> list:
    """List tables through the legacy route, or views through their paginated API."""
    if kind == 'views':
        return query_entities_paginated(kind, database)
    response = requests.get(f'{HOST}/hts/{kind}/query', params={'databaseId': database})
    assert_status(response, 200, f"querying {kind} in {database}")
    body = response.json()
    assert body['pageResults'] is None, \
        f"unpaginated query must not populate pageResults. {describe(response)}"
    return body['results']


def query_entities_paginated(kind: str, database: str) -> list:
    """Read all pages before callers mutate rows (especially during cleanup)."""
    entities = []
    page = 0
    while True:
        response = requests.get(
            f'{HOST}/v1/hts/{kind}/query',
            params={'databaseId': database, 'page': page, 'size': 50, 'sortBy': 'tableId'})
        assert_status(response, 200, f"paginated query of {kind} in {database}, page {page}")
        body = response.json()
        assert body['results'] is None, \
            f"paginated query must not populate results. {describe(response)}"
        result = body['pageResults']
        assert result['number'] == page, f"unexpected page number: {describe(response)}"
        assert isinstance(result['last'], bool), f"missing last-page flag: {describe(response)}"
        entities.extend(result['content'])
        if result['last']:
            return entities
        assert result['content'], f"empty non-final page: {describe(response)}"
        page += 1


def query_soft_deleted(database: str, table: str = None) -> list:
    params = {'databaseId': database}
    if table is not None:
        params['tableId'] = table
    response = requests.get(f'{HOST}/hts/tables/querySoftDeleted', params=params)
    assert_status(response, 200, f"querying soft deleted tables in {database}")
    return response.json()['pageResults']['content']


def soft_delete_table(database: str, table: str) -> requests.Response:
    return requests.delete(f'{HOST}/v1/hts/tables',
                           params={'databaseId': database, 'tableId': table,
                                   'isSoftDelete': 'true'})


def rename_table(database: str, from_table: str, to_table: str,
                 metadata_location: str = None) -> requests.Response:
    return requests.patch(f'{HOST}/hts/tables/rename', params={
        'fromDatabaseId': database,
        'fromTableId': from_table,
        'toDatabaseId': database,
        'toTableId': to_table,
        'metadataLocation': metadata_location or f'/tmp/{database}/{to_table}',
    })


def table_ids(entities: list) -> set:
    return {entity['tableId'] for entity in entities}


def cleanup(database: str) -> None:
    """Remove this suite's rows from its run-scoped database.

    View cleanup collects all pages before deleting. Soft-delete cleanup reads
    one page, sufficient for these fixtures; it is not a general bulk cleanup.
    """
    for entity in query_entities('views', database):
        delete_entity('views', database, entity['tableId'])
    for entity in query_entities('tables', database):
        delete_entity('tables', database, entity['tableId'])
    for entity in query_soft_deleted(database):
        requests.delete(f'{HOST}/hts/tables/purge',
                        params={'databaseId': database, 'tableId': entity['tableId']})


# --------------------------------------------------------------------------- #
# Entity type discriminator: creation and endpoint/payload agreement
# --------------------------------------------------------------------------- #

def test_put_table_defaults_entity_type_to_table() -> None:
    database = database_id('put_table')
    try:
        response = put_entity('tables', database, 't1')
        assert_status(response, 201, "PUT /hts/tables without entityType")
        entity = response.json()['entity']
        assert entity['entityType'] == 'TABLE', \
            f"endpoint must stamp TABLE, got {entity['entityType']!r}. {describe(response)}"
        assert entity['databaseId'] == database and entity['tableId'] == 't1', \
            f"echoed key does not match request. {describe(response)}"
        print("PUT /hts/tables defaults entityType to TABLE")
    finally:
        cleanup(database)


def test_put_view_sets_entity_type_view() -> None:
    database = database_id('put_view')
    try:
        response = put_entity('views', database, 'v1')
        assert_status(response, 201, "PUT /hts/views without entityType")
        entity = response.json()['entity']
        assert entity['entityType'] == 'VIEW', \
            f"endpoint must stamp VIEW, got {entity['entityType']!r}. {describe(response)}"
        print("PUT /hts/views stamps entityType VIEW")
    finally:
        cleanup(database)


def test_put_lowercase_entity_type_is_normalized() -> None:
    database = database_id('lowercase_type')
    try:
        response = put_entity('views', database, 'v1', entity_type='view')
        assert_status(response, 201, "PUT /hts/views with entityType 'view'")
        entity = response.json()['entity']
        assert entity['entityType'] == 'VIEW', \
            f"lowercase agreement must normalize to VIEW, got {entity['entityType']!r}. " \
            f"{describe(response)}"
        print("PUT /hts/views accepts lowercase 'view' and normalizes it to VIEW")
    finally:
        cleanup(database)


def test_put_unknown_entity_type_is_rejected() -> None:
    database = database_id('garbage_type')
    try:
        response = put_entity('tables', database, 't1', entity_type='GARBAGE')
        assert_status(response, 400, "PUT /hts/tables with entityType GARBAGE")
        assert_message(response,
                       'entityType provided: GARBAGE, but this endpoint serves TABLE only',
                       "PUT /hts/tables with entityType GARBAGE")
        assert get_entity('entities', database, 't1').status_code == 404, \
            "a rejected PUT must not have created a row"
        print("PUT /hts/tables rejects an unknown entityType with 400")
    finally:
        cleanup(database)


def test_put_mismatched_entity_type_is_rejected() -> None:
    """A payload may agree with its route or stay silent, never override it."""
    database = database_id('mismatched_type')
    try:
        response = put_entity('tables', database, 't1', entity_type='VIEW')
        assert_status(response, 400, "PUT /hts/tables with entityType VIEW")
        assert_message(response,
                       'entityType provided: VIEW, but this endpoint serves TABLE only',
                       "PUT /hts/tables with entityType VIEW")

        response = put_entity('views', database, 'v1', entity_type='TABLE')
        assert_status(response, 400, "PUT /hts/views with entityType TABLE")
        assert_message(response,
                       'entityType provided: TABLE, but this endpoint serves VIEW only',
                       "PUT /hts/views with entityType TABLE")
        print("PUT rejects a payload entityType that contradicts the endpoint")
    finally:
        cleanup(database)


def test_put_conflicting_entity_type_at_occupied_key() -> None:
    """The key is shared between the two types, so the wrong type is a collision."""
    database = database_id('type_collision')
    try:
        create_view(database, 'shared')
        response = put_entity('tables', database, 'shared')
        assert_status(response, 409, "PUT /hts/tables over an existing view")
        assert_message(response, f'VIEW {database}.shared already exists',
                       "PUT /hts/tables over an existing view")

        survivor = get_entity('views', database, 'shared')
        assert_status(survivor, 200, "the view must survive a rejected table create")
        print("PUT of the wrong type at an occupied key is a 409 and leaves the occupant intact")
    finally:
        cleanup(database)


# --------------------------------------------------------------------------- #
# Reads: /hts/tables and /hts/views are type scoped, /hts/entities is not
# --------------------------------------------------------------------------- #

def test_get_is_type_scoped() -> None:
    database = database_id('get_scoped')
    try:
        create_table(database, 't_tbl')
        create_view(database, 'v_vw')

        response = get_entity('tables', database, 'v_vw')
        assert_status(response, 404, "GET /hts/tables on a view")

        response = get_entity('views', database, 't_tbl')
        assert_status(response, 404, "GET /hts/views on a table")
        assert_message(response, f'View {database}.t_tbl cannot be found',
                       "GET /hts/views on a table")

        response = get_entity('tables', database, 't_tbl')
        assert_status(response, 200, "GET /hts/tables on a table")
        assert response.json()['entity']['entityType'] == 'TABLE', describe(response)

        response = get_entity('views', database, 'v_vw')
        assert_status(response, 200, "GET /hts/views on a view")
        assert response.json()['entity']['entityType'] == 'VIEW', describe(response)
        print("GET /hts/tables and /hts/views each report the other type as not found")
    finally:
        cleanup(database)


def test_get_entities_resolves_either_type() -> None:
    database = database_id('entities')
    try:
        create_table(database, 't_tbl')
        create_view(database, 'v_vw')

        response = get_entity('entities', database, 't_tbl')
        assert_status(response, 200, "GET /hts/entities on a table")
        assert response.json()['entity']['entityType'] == 'TABLE', \
            f"/hts/entities must report the occupant's type. {describe(response)}"

        response = get_entity('entities', database, 'v_vw')
        assert_status(response, 200, "GET /hts/entities on a view")
        assert response.json()['entity']['entityType'] == 'VIEW', \
            f"/hts/entities must report the occupant's type. {describe(response)}"

        response = get_entity('entities', database, 'absent')
        assert_status(response, 404, "GET /hts/entities on a free key")
        assert_message(response, f'Entity {database}.absent cannot be found',
                       "GET /hts/entities on a free key")
        print("GET /hts/entities resolves either type and reports which one it found")
    finally:
        cleanup(database)


def test_neutral_and_typed_endpoints_agree_on_existence() -> None:
    """/hts/entities and the matching typed read must agree that a key exists.

    The invariant: for any given key, the neutral and the typed read must reach
    the same verdict on whether the row is there. A row that /hts/entities
    reports as an existing healthy TABLE while /hts/tables reports 404 is a
    Java/SQL disagreement made directly observable over HTTP, with no SQL client
    needed to see it.

    This test uses only well-formed rows. Corrupt values require separate
    assertions because SQL collation and Java parsing need not agree: a
    trailing-space discriminator is rejected by the Java enum parser even if
    a PAD SPACE collation lets it through the typed SQL predicate.

    ``kind`` is chosen per row deliberately. A view read through /hts/tables is a
    404 by design, so the typed route compared against must match the row's own
    type or the test measures type scoping rather than agreement.

    A 500 is classified as neither exists nor absent -- see ``existence_state``
    for why it is kept as a third state instead of being folded into one.

    LIMITATION, stated honestly: this invariant does not hold universally on this
    deployment. A corrupt discriminator genuinely breaks it, because the typed
    route's predicate misses the row while the neutral route has no predicate and
    hydrates it into the converter. That is a real bug and it is pinned as the
    known-bad case in
    ``test_corrupt_entity_type_values_split_into_two_failure_modes``, not
    smoothed over here. This test asserts the invariant only for well-formed
    rows, which are the rows that must agree on any collation.

    Rows are seeded here over HTTP under a run-scoped database id rather than
    borrowed from a shared fixture, so nothing outside this run can change the
    inputs underneath it.
    """
    database = database_id('agreement')
    try:
        create_table(database, 't_tbl')
        create_view(database, 'v_vw')

        # A healthy table, a healthy view and a free key. These three must agree
        # under any collation, which is what makes them the durable core.
        assert_endpoints_agree(database, 't_tbl', 'tables', "a healthy TABLE")
        assert_endpoints_agree(database, 'v_vw', 'views', "a healthy VIEW")
        assert_endpoints_agree(database, 'absent', 'tables',
                               "a key that was never written")

        # Guard against the invariant passing for the wrong reason. Agreement is
        # only meaningful if the agreed state is the true one, so pin that the
        # healthy rows agree on EXISTS rather than agreeing on absent or error.
        _, neutral_state, typed_state, neutral, typed = endpoints_agree_on_existence(
            database, 't_tbl', 'tables')
        assert neutral_state == typed_state == EXISTS, \
            f"a healthy TABLE must agree on {EXISTS}, got {neutral_state}/{typed_state}. " \
            f"{describe(neutral)} || {describe(typed)}"
        print("/hts/entities and the matching typed read agree on existence for well-formed rows")
    finally:
        cleanup(database)


def test_query_endpoints_are_type_scoped() -> None:
    database = database_id('query_scoped')
    try:
        create_table(database, 't_tbl')
        create_view(database, 'v_vw')

        tables = query_entities('tables', database)
        assert table_ids(tables) == {'t_tbl'}, \
            f"/hts/tables/query must list TABLE rows only, got {table_ids(tables)}"
        assert all(entity['entityType'] == 'TABLE' for entity in tables), tables

        views = query_entities('views', database)
        assert table_ids(views) == {'v_vw'}, \
            f"/v1/hts/views/query must list VIEW rows only, got {table_ids(views)}"
        assert all(entity['entityType'] == 'VIEW' for entity in views), views
        print("/hts/tables/query and /v1/hts/views/query each list only their own type")
    finally:
        cleanup(database)


def test_v1_query_returns_page_results() -> None:
    database = database_id('paginated')
    try:
        create_table(database, 't_tbl')
        create_view(database, 'v_vw')

        views = query_entities_paginated('views', database)
        assert table_ids(views) == {'v_vw'}, \
            f"/v1/hts/views/query must page VIEW rows only, got {table_ids(views)}"

        tables = query_entities_paginated('tables', database)
        assert table_ids(tables) == {'t_tbl'}, \
            f"/v1/hts/tables/query must page TABLE rows only, got {table_ids(tables)}"
        print("/v1 query endpoints populate pageResults.content and stay type scoped")
    finally:
        cleanup(database)


def test_view_pagination_and_cleanup_cover_multiple_pages() -> None:
    database = database_id('view_pages')
    expected = {f'v_{number:03d}' for number in range(51)}
    try:
        for table in sorted(expected):
            create_view(database, table)
        views = query_entities('views', database)
        assert len(views) == len(expected) and table_ids(views) == expected, \
            "view listing must include every page without duplicates"
    finally:
        cleanup(database)
    assert query_entities('views', database) == [], \
        "cleanup must delete views beyond the first page"
    print("view listing and cleanup cover more than one page")


# --------------------------------------------------------------------------- #
# Updates: tableVersion is a compare-and-swap token
# --------------------------------------------------------------------------- #

def test_update_requires_current_metadata_location() -> None:
    database = database_id('cas')
    try:
        created = create_table(database, 't1')
        current = created['metadataLocation']

        response = put_entity('tables', database, 't1', table_version=current,
                              metadata_location=f'{current}_v2')
        assert_status(response, 200, "PUT with the current metadataLocation as tableVersion")
        assert response.json()['entity']['metadataLocation'] == f'{current}_v2', \
            describe(response)

        # The token is now stale: it names the location the update just replaced.
        response = put_entity('tables', database, 't1', table_version=current,
                              metadata_location=f'{current}_v3')
        assert_status(response, 409, "PUT with a stale tableVersion")

        response = put_entity('tables', database, 't1', table_version=INITIAL_VERSION)
        assert_status(response, 409, "PUT with INITIAL_VERSION over an existing row")
        print("PUT treats tableVersion as a compare-and-swap token on metadataLocation")
    finally:
        cleanup(database)


# --------------------------------------------------------------------------- #
# Deletes
# --------------------------------------------------------------------------- #

def test_delete_table_is_table_scoped() -> None:
    database = database_id('delete_table')
    try:
        create_view(database, 'v_vw')

        response = delete_entity('tables', database, 'v_vw')
        assert_status(response, 404, "DELETE /hts/tables on a view")

        assert_status(get_entity('views', database, 'v_vw'), 200,
                      "the view must survive a table-scoped delete")
        print("DELETE /hts/tables reports a view as not found and leaves it in place")
    finally:
        cleanup(database)


def test_delete_view_removes_only_the_view() -> None:
    database = database_id('delete_view')
    try:
        create_table(database, 'sibling')
        create_view(database, 'v_vw')

        response = delete_entity('views', database, 'v_vw')
        assert_status(response, 204, "DELETE /hts/views on a view")
        assert response.text == '', f"204 must carry no body, got {response.text!r}"

        assert_status(get_entity('views', database, 'v_vw'), 404,
                      "the view must be gone after DELETE /hts/views")
        assert_status(get_entity('tables', database, 'sibling'), 200,
                      "a sibling table must be untouched by a view delete")

        response = delete_entity('views', database, 'v_vw')
        assert_status(response, 404, "DELETE /hts/views on an already deleted view")
        print("DELETE /hts/views removes the view and nothing else")
    finally:
        cleanup(database)


# --------------------------------------------------------------------------- #
# Rename: PATCH /hts/tables/rename
# --------------------------------------------------------------------------- #

def test_rename_table_succeeds() -> None:
    database = database_id('rename_ok')
    try:
        create_table(database, 'before')

        response = rename_table(database, 'before', 'after',
                                metadata_location=f'/tmp/{database}/after')
        assert_status(response, 204, "PATCH /hts/tables/rename on a table")

        assert_status(get_entity('tables', database, 'before'), 404,
                      "the source key must be free after a rename")
        renamed = get_entity('tables', database, 'after')
        assert_status(renamed, 200, "the destination key must hold the renamed table")
        entity = renamed.json()['entity']
        assert entity['metadataLocation'] == f'/tmp/{database}/after', describe(renamed)
        assert entity['entityType'] == 'TABLE', \
            f"a rename must preserve the discriminator. {describe(renamed)}"
        print("PATCH /hts/tables/rename moves a table and preserves entityType TABLE")
    finally:
        cleanup(database)


def test_rename_is_table_scoped() -> None:
    """Renaming a view through the table endpoint must match zero rows."""
    database = database_id('rename_scoped')
    try:
        create_view(database, 'v_vw')

        response = rename_table(database, 'v_vw', 'v_renamed')
        assert_status(response, 404, "PATCH /hts/tables/rename on a view")
        assert_message(response, f'User table {database}.v_vw cannot be found',
                       "PATCH /hts/tables/rename on a view")

        assert_status(get_entity('views', database, 'v_vw'), 200,
                      "the view must survive a table-scoped rename")
        assert_status(get_entity('entities', database, 'v_renamed'), 404,
                      "a table-scoped rename must not create the destination")
        print("PATCH /hts/tables/rename reports a view as not found and moves nothing")
    finally:
        cleanup(database)


def test_rename_onto_occupied_destination_conflicts() -> None:
    database = database_id('rename_conflict')
    try:
        create_table(database, 'src')
        create_table(database, 'occupied_tbl')
        create_view(database, 'occupied_vw')

        response = rename_table(database, 'src', 'occupied_tbl')
        assert_status(response, 409, "rename onto a key held by a table")
        assert_message(response, 'Table occupied_tbl already exists',
                       "rename onto a key held by a table")

        response = rename_table(database, 'src', 'occupied_vw')
        assert_status(response, 409, "rename onto a key held by a view")
        assert_message(response, 'Table occupied_vw already exists',
                       "rename onto a key held by a view")

        assert_status(get_entity('tables', database, 'src'), 200,
                      "a rejected rename must leave the source in place")
        occupant = get_entity('views', database, 'occupied_vw')
        assert_status(occupant, 200, "a rejected rename must leave the occupant in place")
        assert occupant.json()['entity']['metadataLocation'] == f'/tmp/{database}/occupied_vw', \
            f"the occupant's metadataLocation must not be overwritten. {describe(occupant)}"
        print("PATCH /hts/tables/rename onto an occupied key is a 409 for either occupant type")
    finally:
        cleanup(database)


# --------------------------------------------------------------------------- #
# Soft delete lifecycle
# --------------------------------------------------------------------------- #

def test_restore_stamps_the_table_discriminator() -> None:
    """Verify restore persists TABLE rather than relying on the NULL read default.

    The soft-deleted store has no discriminator column. Restore maps that row
    to a live TABLE through UserTablesMapper; the current converter would reject
    a missing stamp rather than persist NULL. The raw-column assertion also
    protects against a future write path allowing NULL, since a subsequent API
    read would resolve that NULL to TABLE and hide the storage error.

    Requires a matching MySQL connection. API-visible restore behavior is
    separately covered by test_soft_delete_restore_and_purge_lifecycle.
    """
    connection = require_database()
    database = database_id('restore_discriminator')
    try:
        create_table(database, 't_sd')
        stored, is_null = read_entity_type_column(connection, database, 't_sd')
        assert stored == 'TABLE' and is_null == 0, \
            f"a created table must store a literal TABLE, got {stored!r}"

        assert_status(soft_delete_table(database, 't_sd'), 204, "soft deleting the table")
        assert count_rows(connection, database, 't_sd') == 0, \
            "a soft deleted table must leave user_table_row"

        soft_deleted = query_soft_deleted(database, 't_sd')
        assert len(soft_deleted) == 1, \
            f"expected exactly one soft deleted row, got {soft_deleted}"
        row = soft_deleted[0]

        # Deliberate, not an oversight: there is no column behind this field. Do
        # not "fix" it to TABLE -- doing so would mask a real regression in the
        # restore stamp below.
        assert row['entityType'] is None, \
            f"the soft deleted projection has no entity_type column and must serialize " \
            f"entityType null, got {row['entityType']!r} in {row}"

        # Read deletedAtMs off the response rather than guessing a timestamp.
        deleted_at_ms = row['deletedAtMs']
        assert deleted_at_ms is not None, f"deletedAtMs must be stamped, got {row}"

        response = requests.put(f'{HOST}/hts/tables/restore', params={
            'databaseId': database, 'tableId': 't_sd', 'deletedAtMs': deleted_at_ms})
        assert_status(response, 200, "PUT /hts/tables/restore")

        # The assertion this test exists for.
        stored, is_null = read_entity_type_column(connection, database, 't_sd')
        assert is_null == 0, \
            "restore must store TABLE explicitly, not rely on EntityTypeConverter's " \
            "legacy NULL-to-TABLE read default"
        assert stored == 'TABLE', \
            f"restore must persist the literal string TABLE, got {stored!r}"

        # Secondary. True of a stored NULL too, so it proves much less.
        assert response.json()['entity']['entityType'] == 'TABLE', \
            f"restore must stamp TABLE back onto a row that lost its discriminator. " \
            f"{describe(response)}"

        response = get_entity('tables', database, 't_sd')
        assert_status(response, 200, "GET /hts/tables on the restored row")
        assert response.json()['entity']['entityType'] == 'TABLE', \
            f"the restored row must read back as a TABLE. {describe(response)}"
        print("restore persists a literal TABLE into entity_type, not a NULL")
    finally:
        cleanup(database)
        purge_database_rows(connection, database)
        connection.close()


def test_soft_delete_restore_and_purge_lifecycle() -> None:
    database = database_id('soft_delete')
    try:
        created = create_table(database, 't1')
        metadata_location = created['metadataLocation']

        response = soft_delete_table(database, 't1')
        assert_status(response, 204, "DELETE /v1/hts/tables?isSoftDelete=true")
        assert_status(get_entity('tables', database, 't1'), 404,
                      "a soft deleted table must leave the live store")

        soft_deleted = query_soft_deleted(database, 't1')
        assert len(soft_deleted) == 1, \
            f"expected exactly one soft deleted row, got {soft_deleted}"
        row = soft_deleted[0]
        assert row['tableId'] == 't1' and row['databaseId'] == database, row
        assert row['metadataLocation'] == metadata_location, row
        assert row['deletedAtMs'] is not None, f"deletedAtMs must be stamped, got {row}"
        assert row['purgeAfterMs'] is not None and row['purgeAfterMs'] > row['deletedAtMs'], \
            f"purgeAfterMs must be a TTL beyond deletedAtMs, got {row}"
        deleted_at_ms = row['deletedAtMs']

        response = requests.put(f'{HOST}/hts/tables/restore', params={
            'databaseId': database, 'tableId': 't1', 'deletedAtMs': deleted_at_ms})
        assert_status(response, 200, "PUT /hts/tables/restore")
        restored = response.json()['entity']
        assert restored['entityType'] == 'TABLE', \
            f"a restored row must resolve as a TABLE, got {restored}"
        assert restored['metadataLocation'] == metadata_location, restored

        assert_status(get_entity('tables', database, 't1'), 200,
                      "a restored table must be back in the live store")
        assert query_soft_deleted(database, 't1') == [], \
            "a restored table must leave the soft deleted store"

        # Restoring onto a key that is occupied again is a conflict.
        response = requests.put(f'{HOST}/hts/tables/restore', params={
            'databaseId': database, 'tableId': 't1', 'deletedAtMs': deleted_at_ms})
        assert_status(response, 409, "PUT /hts/tables/restore over a live table")

        assert_status(soft_delete_table(database, 't1'), 204, "second soft delete")
        assert len(query_soft_deleted(database, 't1')) == 1, "re-soft-delete must record a row"

        response = requests.delete(f'{HOST}/hts/tables/purge',
                                   params={'databaseId': database, 'tableId': 't1'})
        assert_status(response, 204, "DELETE /hts/tables/purge")
        assert query_soft_deleted(database, 't1') == [], \
            "a purged table must leave the soft deleted store for good"
        assert_status(get_entity('entities', database, 't1'), 404,
                      "a purged table must not reappear in the live store")

        response = requests.put(f'{HOST}/hts/tables/restore', params={
            'databaseId': database, 'tableId': 't1', 'deletedAtMs': deleted_at_ms})
        assert_status(response, 404, "PUT /hts/tables/restore after a purge")
        print("soft delete -> querySoftDeleted -> restore -> purge behaves end to end")
    finally:
        cleanup(database)


def test_view_drop_produces_no_soft_deleted_row() -> None:
    """Views are always hard deleted: there is no soft-deleted view store.

    Both halves matter. The table-scoped soft-delete route refuses a view with a
    404 rather than silently soft-deleting it, and a hard-dropped view leaves
    ``querySoftDeleted`` empty for that key.
    """
    database = database_id('view_no_soft_delete')
    try:
        create_view(database, 'v_sd')
        create_table(database, 'sibling')

        response = soft_delete_table(database, 'v_sd')
        assert_status(response, 404, "DELETE /v1/hts/tables?isSoftDelete=true on a view")
        assert_message(response, f'User table {database}.v_sd cannot be found',
                       "DELETE /v1/hts/tables?isSoftDelete=true on a view")
        assert_status(get_entity('views', database, 'v_sd'), 200,
                      "a refused soft delete must leave the view in place")
        assert query_soft_deleted(database, 'v_sd') == [], \
            "a refused soft delete must not record a soft deleted row"

        assert_status(delete_entity('views', database, 'v_sd'), 204, "dropping the view")
        assert_status(get_entity('entities', database, 'v_sd'), 404,
                      "the dropped view must be gone from the live store")

        assert query_soft_deleted(database, 'v_sd') == [], \
            "a dropped view must leave no soft deleted row at its own key"
        assert table_ids(query_soft_deleted(database)) == set(), \
            "a dropped view must leave no soft deleted row anywhere in the database"

        # A table dropped the same way does land in the soft deleted store, which
        # shows the empty results above are scoping and not an empty database.
        assert_status(soft_delete_table(database, 'sibling'), 204, "soft deleting the table")
        assert table_ids(query_soft_deleted(database)) == {'sibling'}, \
            "the sibling table must be the only soft deleted row"
        print("a view drop writes no soft deleted row, and the table route refuses a view")
    finally:
        cleanup(database)


# --------------------------------------------------------------------------- #
# Database-backed cases. These reach past the API to plant or inspect a column
# that no request can express, and skip when no database is reachable.
# --------------------------------------------------------------------------- #

def test_legacy_null_entity_type_resolves_as_table() -> None:
    """A row predating the discriminator must behave as a TABLE everywhere.

    The delete pair at the end is the most valuable assertion in this file. The
    delete predicate is a separate code path from the read predicate and carries
    its own null arm. If either is ever "simplified" to a plain
    ``entity_type = 'TABLE'`` equality, every pre-existing legacy table becomes
    invisible or undeletable. This test checks both paths against the deployment.

    The row is planted with raw SQL because ingress stamping means no request can
    produce a NULL discriminator.
    """
    connection = require_database()
    database = database_id('legacy_null')
    try:
        plant_legacy_null_row(connection, database, 't_legacy')

        # Prove the column really is NULL rather than merely absent from a
        # response body. That distinction has been confused before, and every
        # assertion below is worthless without it.
        stored, is_null = read_entity_type_column(connection, database, 't_legacy')
        assert is_null == 1 and stored is None, \
            f"the planted row must hold a SQL NULL discriminator, got {stored!r}"

        response = get_entity('tables', database, 't_legacy')
        assert_status(response, 200, "GET /hts/tables on a legacy NULL row")
        assert response.json()['entity']['entityType'] == 'TABLE', \
            f"a NULL discriminator must read as TABLE. {describe(response)}"

        response = get_entity('views', database, 't_legacy')
        assert_status(response, 404, "GET /hts/views on a legacy NULL row")

        tables = query_entities('tables', database)
        assert table_ids(tables) == {'t_legacy'}, \
            f"/hts/tables/query must include a legacy NULL row, got {table_ids(tables)}"
        assert tables[0]['entityType'] == 'TABLE', \
            f"the query projection must resolve NULL to TABLE, got {tables[0]}"

        views = query_entities('views', database)
        assert table_ids(views) == set(), \
            f"/v1/hts/views/query must exclude a legacy NULL row, got {table_ids(views)}"

        response = delete_entity('views', database, 't_legacy')
        assert_status(response, 404, "DELETE /hts/views on a legacy NULL row")
        assert count_rows(connection, database, 't_legacy') == 1, \
            "a view-scoped delete must not remove a legacy NULL row"

        response = delete_entity('tables', database, 't_legacy')
        assert_status(response, 204, "DELETE /hts/tables on a legacy NULL row")
        assert count_rows(connection, database, 't_legacy') == 0, \
            "a legacy NULL row must be deletable through the table endpoint"
        print("a legacy NULL entity_type reads, queries and deletes as a TABLE")
    finally:
        cleanup(database)
        purge_database_rows(connection, database)
        connection.close()


def test_corrupt_entity_type_values_split_into_two_failure_modes() -> None:
    """A corrupt discriminator fails in one of two quite different ways.

    The read predicate compares UPPER(entity_type) against 'TABLE' under the column's collation, so
    the collation decides which corrupt values are even seen:

    * Values the collation calls *equal* to 'TABLE' pass the predicate and reach
      ``EntityTypeConverter``, which rejects them and produces a 500. Under
      utf8mb4_0900_ai_ci that includes accented forms such as 'TABLE' with an
      acute accent, because the collation is accent insensitive.
    * Everything else fails the predicate, so the row is never selected and the
      typed API reports 404. NO PAD makes trailing spaces significant; leading
      spaces, the empty string and an all-spaces value also fail the predicate.
      Such a row is invisible to the typed routes, but it is
      not benign: the neutral /hts/entities read carries no type predicate, so
      it hydrates the row and fails the same way an accented value does.

    Invisible and broken are very different operationally, and neither is
    obvious from reading the converter alone, so both are pinned here.

    The 500 body is asserted on both routes, not just one. They reach the
    converter by different paths -- the typed route only for a value the
    predicate matches, the neutral route for any value at all because it carries
    no type predicate -- so one carrying the diagnostic does not imply the other
    does. The assertion matches the parts that carry meaning (the column name and
    the quoted offending value) rather than the exact string, so rewording the
    prose around them does not break it.
    """
    connection = require_database()
    database = database_id('corrupt')

    # Pin the two collations this test reasons about, so the pinning in
    # database_connection cannot silently rot. The column's collation is the one
    # that decides the comparisons below, because it outranks a literal's.
    connection_collation = query_one(connection, 'SELECT @@collation_connection', ())[0]
    assert connection_collation == 'utf8mb4_0900_ai_ci', \
        f"expected a utf8mb4_0900_ai_ci connection, got {connection_collation}. " \
        f"A latin1 connection compares under PAD SPACE and answers ad hoc padding " \
        f"checks incorrectly."
    column_collation = query_one(
        connection,
        "SELECT collation_name FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = 'user_table_row' "
        "AND column_name = 'entity_type'", ())[0]
    assert column_collation == 'utf8mb4_0900_ai_ci', \
        f"this test's expectations assume an accent insensitive NO PAD column " \
        f"collation, got {column_collation}. The taxonomy below changes with it."

    # table id, stored value, expected status, what the case demonstrates
    cases = [
        ('t_clean', 'TABLE', 200, "the valid control"),
        ('t_accented', 'T\u00c1BLE', 500, "accent insensitive match reaches the converter"),
        ('t_trailing', 'TABLE ', 404, "NO PAD collation, so a trailing space does not match"),
        ('t_leading', ' TABLE', 404, "a leading space does not match"),
        ('t_empty', '', 404, "the empty string does not match"),
        ('t_spaces', '   ', 404, "an all spaces value does not match"),
    ]
    try:
        for table, stored, _, _ in cases:
            plant_entity_type(connection, database, table, stored)

        for table, stored, expected_status, rationale in cases:
            # Confirm what the database itself thinks before blaming the service.
            expected_match = 0 if expected_status == 404 else 1
            matches = query_one(
                connection,
                "SELECT entity_type = 'TABLE' FROM user_table_row "
                "WHERE database_id = %s AND table_id = %s",
                (database, table))[0]
            assert matches == expected_match, \
                f"expected the read predicate to give {expected_match} for {stored!r} " \
                f"({rationale}), got {matches}"

            response = get_entity('tables', database, table)
            assert_status(response, expected_status,
                          f"GET /hts/tables on entity_type {stored!r}: {rationale}")
            if expected_status == 500:
                # Predicate-matched path: the typed route selected the row and
                # the converter rejected it.
                assert_corrupt_value_diagnostic(
                    response, stored,
                    f"GET /hts/tables on entity_type {stored!r}")

            # The neutral route carries no type predicate, so every corrupt value
            # reaches the converter here, including the ones the typed route
            # never selects. This is the predicate-missed path into the same
            # failure, and it is asserted separately because a fix to one
            # unwrapping path does not imply the other.
            if stored != 'TABLE':
                response = get_entity('entities', database, table)
                assert_status(response, 500,
                              f"GET /hts/entities on entity_type {stored!r}: {rationale}")
                assert_corrupt_value_diagnostic(
                    response, stored,
                    f"GET /hts/entities on entity_type {stored!r}")

        # KNOWN BAD. The endpoint-agreement invariant asserted for well-formed
        # rows in test_neutral_and_typed_endpoints_agree_on_existence does not
        # hold for a corrupt discriminator, and that is a real bug rather than a
        # quirk of this test. Pinned as an explicit disagreement so it is visible
        # and so a fix that makes the routes agree fails here loudly instead of
        # passing silently.
        #
        # Collation limitation: this disagreement exists because
        # utf8mb4_0900_ai_ci is NO PAD, so 'TABLE ' fails the typed route's
        # predicate (404) while the neutral route still hydrates it (500). Under
        # a PAD SPACE collation both routes would reach the converter and answer
        # 500: EntityType.fromName does not trim the stored value. The
        # column collation is pinned at the top of this test, so if that ever
        # changes this assertion fails and says so rather than quietly inverting.
        agree, neutral_state, typed_state, neutral, typed = endpoints_agree_on_existence(
            database, 't_trailing', 'tables')
        assert not agree and typed_state == ABSENT and neutral_state == ERROR, \
            f"expected the known NO PAD disagreement on a trailing-space " \
            f"discriminator: /hts/tables {ABSENT} vs /hts/entities {ERROR}, got " \
            f"{typed_state} vs {neutral_state}. If the routes now agree, the " \
            f"underlying bug may be fixed -- update this and the invariant test " \
            f"together. {describe(typed)} || {describe(neutral)}"

        # By contrast an accented value fails the same way on both routes, so it
        # agrees (both ERROR). That is the honest reading: there is no Java/SQL
        # disagreement here, just a row neither route can convert.
        agree, neutral_state, typed_state, _, _ = endpoints_agree_on_existence(
            database, 't_accented', 'tables')
        assert agree and neutral_state == typed_state == ERROR, \
            f"an accent-insensitive match should reach the converter on both " \
            f"routes and agree on {ERROR}, got {typed_state} vs {neutral_state}"

        # Blast radius: one unconvertible row fails the whole database listing,
        # not just its own key, and carries the same diagnostic.
        response = requests.get(f'{HOST}/hts/tables/query', params={'databaseId': database})
        assert_status(response, 500,
                      "GET /hts/tables/query over a database holding one unconvertible row")
        assert_message_contains(
            response, ('user_table_row.entity_type',),
            "GET /hts/tables/query over a database holding one unconvertible row")
        print("corrupt entity_type values split into 500 (reaches converter) and 404 (invisible)")
    finally:
        purge_database_rows(connection, database)
        connection.close()


# --------------------------------------------------------------------------- #
# Request validation. HTS is unauthenticated, so a bad request is 400, not 401.
# --------------------------------------------------------------------------- #
def test_missing_required_parameters_are_bad_request() -> None:
    unauthenticated_requests = [
        ('GET /hts/tables without params', requests.get(f'{HOST}/hts/tables')),
        ('GET /hts/views without params', requests.get(f'{HOST}/hts/views')),
        ('GET /hts/entities without params', requests.get(f'{HOST}/hts/entities')),
        ('GET /hts/tables/querySoftDeleted without databaseId',
         requests.get(f'{HOST}/hts/tables/querySoftDeleted')),
        ('PATCH /hts/tables/rename without params',
         requests.patch(f'{HOST}/hts/tables/rename')),
        ('DELETE /v1/hts/tables without isSoftDelete',
         requests.delete(f'{HOST}/v1/hts/tables',
                         params={'databaseId': 'd', 'tableId': 't'})),
    ]
    for what, response in unauthenticated_requests:
        assert_status(response, 400, what)
    print("missing required parameters return 400 on an unauthenticated service")


TESTS = [
    test_put_table_defaults_entity_type_to_table,
    test_put_view_sets_entity_type_view,
    test_put_lowercase_entity_type_is_normalized,
    test_put_unknown_entity_type_is_rejected,
    test_put_mismatched_entity_type_is_rejected,
    test_put_conflicting_entity_type_at_occupied_key,
    test_get_is_type_scoped,
    test_get_entities_resolves_either_type,
    test_neutral_and_typed_endpoints_agree_on_existence,
    test_query_endpoints_are_type_scoped,
    test_v1_query_returns_page_results,
    test_view_pagination_and_cleanup_cover_multiple_pages,
    test_update_requires_current_metadata_location,
    test_delete_table_is_table_scoped,
    test_delete_view_removes_only_the_view,
    test_rename_table_succeeds,
    test_rename_is_table_scoped,
    test_rename_onto_occupied_destination_conflicts,
    test_restore_stamps_the_table_discriminator,
    test_soft_delete_restore_and_purge_lifecycle,
    test_view_drop_produces_no_soft_deleted_row,
    test_legacy_null_entity_type_resolves_as_table,
    test_corrupt_entity_type_values_split_into_two_failure_modes,
    test_missing_required_parameters_are_bad_request,
]


if __name__ == '__main__':
    if len(sys.argv) > 2:
        print("Usage: python hts_integration_test.py [host]")
        sys.exit(1)
    if len(sys.argv) == 2:
        HOST = sys.argv[1].rstrip('/')

    print(f"Running {len(TESTS)} HTS integration tests against {HOST} (run id {RUN_ID})")
    passed = 0
    skipped = []
    for test in TESTS:
        try:
            test()
            passed += 1
        except SkippedTest as reason:
            skipped.append(test.__name__)
            print(f"SKIPPED {test.__name__}: {reason}")

    if skipped:
        print(f"{passed} passed, {len(skipped)} skipped: {', '.join(skipped)}")
    else:
        print(f"{passed} passed, 0 skipped")
    print("All tests passed successfully")
