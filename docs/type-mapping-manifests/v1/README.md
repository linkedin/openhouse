# Type Mapping Manifest — v1

Covers Hive 1.1 (HMS), Iceberg 1.2 (table format v2), Spark 3.1, Trino 400.
HiveQL and PyArrow not yet covered.

In each engine cell, the top line is `native → engine` (column created via the
storage format's native API, read by the engine); the bottom line is
`engine → native` (column authored via engine DDL, persisted in storage).
`—` = no equivalent. `⚠` = silent divergence. `❌` = DDL rejected.
`_TBD_` = not yet observed.

## Boolean

| Logical type | Storage    | Spark                                          | Trino                                          |
|--------------|------------|------------------------------------------------|------------------------------------------------|
| boolean      | Iceberg v2 | `boolean` → `boolean`<br>`BOOLEAN` → `boolean` | `boolean` → `boolean`<br>`BOOLEAN` → `boolean` |
| boolean      | Hive (HMS) | _TBD_<br>`BOOLEAN` → `boolean`                 | `boolean` → `boolean`<br>`BOOLEAN` → `boolean` |

## Integers

| Logical type            | Storage    | Spark                      | Trino                                              |
|-------------------------|------------|----------------------------|----------------------------------------------------|
| 8-bit int (`tinyint`)   | Iceberg v2 | —<br>_TBD_                 | —<br>`TINYINT` → ❌ rejected                        |
| 8-bit int (`tinyint`)   | Hive (HMS) | _TBD_<br>_TBD_             | `tinyint` → `tinyint`<br>`TINYINT` → `tinyint`     |
| 16-bit int (`smallint`) | Iceberg v2 | —<br>_TBD_                 | —<br>`SMALLINT` → ❌ rejected                       |
| 16-bit int (`smallint`) | Hive (HMS) | _TBD_<br>_TBD_             | `smallint` → `smallint`<br>`SMALLINT` → `smallint` |
| 32-bit int              | Iceberg v2 | `int` → `int`<br>_TBD_     | `int` → `integer`<br>`INTEGER` → `int`             |
| 32-bit int              | Hive (HMS) | _TBD_<br>_TBD_             | `int` → `integer`<br>`INTEGER` → `int`             |
| 64-bit int              | Iceberg v2 | `long` → `bigint`<br>_TBD_ | `long` → `bigint`<br>`BIGINT` → `long`             |
| 64-bit int              | Hive (HMS) | _TBD_<br>_TBD_             | `bigint` → `bigint`<br>`BIGINT` → `bigint`         |

## Floats

| Logical type | Storage    | Spark                                      | Trino                                      |
|--------------|------------|--------------------------------------------|--------------------------------------------|
| 32-bit float | Iceberg v2 | `float` → `float`<br>`FLOAT` → `float`     | `float` → `real`<br>`REAL` → `float`       |
| 32-bit float | Hive (HMS) | _TBD_<br>`FLOAT` → `float`                 | `float` → `real`<br>`REAL` → `float`       |
| 64-bit float | Iceberg v2 | `double` → `double`<br>`DOUBLE` → `double` | `double` → `double`<br>`DOUBLE` → `double` |
| 64-bit float | Hive (HMS) | _TBD_<br>`DOUBLE` → `double`               | `double` → `double`<br>`DOUBLE` → `double` |

## Decimal

| Logical type   | Storage    | Spark                                      | Trino          |
|----------------|------------|--------------------------------------------|----------------|
| `decimal(p,s)` | Iceberg v2 | _TBD_<br>`DECIMAL(10,2)` → `decimal(10,2)` | _TBD_<br>_TBD_ |
| `decimal(p,s)` | Hive (HMS) | _TBD_<br>`DECIMAL(10,2)` → `decimal(10,2)` | _TBD_<br>_TBD_ |

## Strings

| Logical type           | Storage    | Spark                                         | Trino                                                        |
|------------------------|------------|-----------------------------------------------|--------------------------------------------------------------|
| Variable-length string | Iceberg v2 | `string` → `string`<br>`STRING` → `string`    | `string` → `varchar`<br>`VARCHAR` → `string`                 |
| Variable-length string | Hive (HMS) | _TBD_<br>`STRING` → `string`                  | `string` → `varchar`<br>`VARCHAR` → `string`                 |
| `VARCHAR(N)`           | Iceberg v2 | —<br>`VARCHAR(100)` → `string` ⚠ bound erased | —<br>`VARCHAR(10)` → `string` ⚠ bound erased                 |
| `VARCHAR(N)`           | Hive (HMS) | _TBD_<br>`VARCHAR(100)` → `varchar(100)`      | `varchar(N)` → `varchar(N)`<br>`VARCHAR(10)` → `varchar(10)` |
| `CHAR(N)`              | Iceberg v2 | —<br>`CHAR(10)` → `string` ⚠ bound erased     | —<br>`CHAR(10)` → ❌ rejected                                 |
| `CHAR(N)`              | Hive (HMS) | _TBD_<br>`CHAR(10)` → `char(10)`              | `char(N)` → `char(N)`<br>`CHAR(10)` → `char(10)`             |

## Binary and logical overlays

| Logical type    | Storage    | Spark                                           | Trino                                              |
|-----------------|------------|-------------------------------------------------|----------------------------------------------------|
| Variable binary | Iceberg v2 | `binary` → `binary`<br>`BINARY` → `binary`      | `binary` → `varbinary`<br>`VARBINARY` → `binary`   |
| Variable binary | Hive (HMS) | _TBD_<br>`BINARY` → `binary`                    | `binary` → `varbinary`<br>`VARBINARY` → `binary`   |
| `fixed(N)`      | Iceberg v2 | `fixed(16)` → `binary` ⚠ length erased<br>_TBD_ | `fixed(16)` → `varbinary` ⚠ length erased<br>_TBD_ |
| `fixed(N)`      | Hive (HMS) | —<br>—                                          | —<br>—                                             |
| `uuid`          | Iceberg v2 | _TBD_<br>_TBD_                                  | `uuid` → `uuid`<br>_TBD_                           |
| `uuid`          | Hive (HMS) | —<br>—                                          | —<br>—                                             |

## Date and time

| Logical type        | Storage    | Spark                                                                       | Trino                                                     |
|---------------------|------------|-----------------------------------------------------------------------------|-----------------------------------------------------------|
| `date`              | Iceberg v2 | _TBD_<br>`DATE` → `date`                                                    | _TBD_<br>`DATE` → `date`                                  |
| `date`              | Hive (HMS) | _TBD_<br>`DATE` → `date`                                                    | _TBD_<br>`DATE` → `date`                                  |
| `time`              | Iceberg v2 | _TBD_<br>_TBD_                                                              | `time` → `time(6)`<br>_TBD_                               |
| `time`              | Hive (HMS) | —<br>—                                                                      | —<br>—                                                    |
| `timestamp` (no TZ) | Iceberg v2 | _TBD_<br>⚠ no Spark DDL path (TIMESTAMP silently produces `timestamptz(6)`) | `timestamp(6)` → `timestamp(6)`<br>_TBD_                  |
| `timestamp` (no TZ) | Hive (HMS) | _TBD_<br>`TIMESTAMP` → `timestamp`                                          | `timestamp` → `timestamp(3)`<br>_TBD_                     |
| `timestamp` with TZ | Iceberg v2 | _TBD_<br>`TIMESTAMP` → `timestamptz(6)` ⚠ silent TZ injection               | `timestamptz(6)` → `timestamp(6) with time zone`<br>_TBD_ |
| `timestamp` with TZ | Hive (HMS) | —<br>—                                                                      | —<br>—                                                    |

## Trino-only types

These types exist in Trino's type system but have no equivalent in either Hive
or Iceberg storage; DDL targeting either format is rejected at creation time.

| Logical type | Storage | Spark | Trino                         |
|--------------|---------|-------|-------------------------------|
| `json`       | —       | —     | —<br>`JSON` → ❌ rejected      |
| `ipaddress`  | —       | —     | —<br>`IPADDRESS` → ❌ rejected |

## Containers

| Logical type | Storage | Spark | Trino |
|---|---|---|---|
| `ARRAY<T>` | Iceberg v2 | `list<element:optional, string>` → `array<element-nullable:yes, string>`<br>`ARRAY<STRING>` → `list<element:optional, string>` | `list<element:required, string>` → `array(varchar)`<br>_TBD_ |
| `ARRAY<T>` | Hive (HMS) | `array<string>` → `array<element-nullable:yes, string>`<br>`ARRAY<STRING>` → `array<string>` | `array<string>` → `array(varchar)`<br>_TBD_ |
| `MAP<K, V>` | Iceberg v2 | `map<key:required, string; value:optional, int>` → `map<string, value-nullable:yes, int>`<br>`MAP<STRING, INT>` → `map<key:required, string; value:optional, int>` | _TBD_<br>_TBD_ |
| `MAP<K, V>` | Hive (HMS) | `map<string, int>` → `map<string, value-nullable:yes, int>`<br>`MAP<STRING, INT>` → `map<string, int>` | _TBD_<br>_TBD_ |
| `STRUCT<…>` | Iceberg v2 | `struct<field:required, value, int; field:optional, label, string>` → `struct<nullable:yes, value, int; nullable:yes, label, string>` ⚠ inner required dropped<br>`STRUCT<value:INT, label:STRING>` → `struct<field:optional, value, int; field:optional, label, string>` | _TBD_<br>_TBD_ |
| `STRUCT<…>` | Hive (HMS) | `struct<value:int, label:string>` → `struct<nullable:yes, value, int; nullable:yes, label, string>`<br>`STRUCT<value:INT, label:STRING>` → `struct<value:int, label:string>` | _TBD_<br>_TBD_ |

## Union

| Logical type             | Storage    | Spark                                                                                                                      | Trino      |
|--------------------------|------------|----------------------------------------------------------------------------------------------------------------------------|------------|
| `UNIONTYPE<int>`         | Iceberg v2 | —<br>—                                                                                                                     | —<br>—     |
| `UNIONTYPE<int>`         | Hive (HMS) | `uniontype<int>` → `int` ⚠ degenerate flattening<br>—                                                                      | _TBD_<br>— |
| `UNIONTYPE<int, string>` | Iceberg v2 | —<br>—                                                                                                                     | —<br>—     |
| `UNIONTYPE<int, string>` | Hive (HMS) | `uniontype<int, string>` → `struct<nullable:no, tag, int; nullable:yes, field0, int; nullable:yes, field1, string>` ⚠<br>— | _TBD_<br>— |
