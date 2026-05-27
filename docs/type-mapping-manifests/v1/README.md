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
| boolean      | Hive (HMS) | _TBD_ → _TBD_<br>`BOOLEAN` → `boolean`         | `boolean` → `boolean`<br>`BOOLEAN` → `boolean` |

## Integers

| Logical type            | Storage    | Spark                               | Trino                                              |
|-------------------------|------------|-------------------------------------|----------------------------------------------------|
| 8-bit int (`tinyint`)   | Iceberg v2 | —<br>—                              | —<br>`TINYINT` → ❌ rejected                        |
| 8-bit int (`tinyint`)   | Hive (HMS) | _TBD_<br>_TBD_                      | `tinyint` → `tinyint`<br>`TINYINT` → `tinyint`     |
| 16-bit int (`smallint`) | Iceberg v2 | —<br>—                              | —<br>`SMALLINT` → ❌ rejected                       |
| 16-bit int (`smallint`) | Hive (HMS) | _TBD_<br>_TBD_                      | `smallint` → `smallint`<br>`SMALLINT` → `smallint` |
| 32-bit int              | Iceberg v2 | `int` → `int`<br>_TBD_ → `int`      | `int` → `integer`<br>`INTEGER` → `int`             |
| 32-bit int              | Hive (HMS) | _TBD_<br>_TBD_                      | `int` → `integer`<br>`INTEGER` → `int`             |
| 64-bit int              | Iceberg v2 | `long` → `bigint`<br>_TBD_ → `long` | `long` → `bigint`<br>`BIGINT` → `long`             |
| 64-bit int              | Hive (HMS) | _TBD_<br>_TBD_                      | `bigint` → `bigint`<br>`BIGINT` → `bigint`         |

## Floats

| Logical type | Storage    | Spark                                   | Trino                                      |
|--------------|------------|-----------------------------------------|--------------------------------------------|
| 32-bit float | Iceberg v2 | `float` → `float`<br>_TBD_ → `float`    | `float` → `real`<br>`REAL` → `float`       |
| 32-bit float | Hive (HMS) | _TBD_<br>_TBD_                          | `float` → `real`<br>`REAL` → `float`       |
| 64-bit float | Iceberg v2 | `double` → `double`<br>_TBD_ → `double` | `double` → `double`<br>`DOUBLE` → `double` |
| 64-bit float | Hive (HMS) | _TBD_<br>_TBD_                          | `double` → `double`<br>`DOUBLE` → `double` |

## Strings

| Logical type           | Storage    | Spark                                    | Trino                                    |
|------------------------|------------|------------------------------------------|------------------------------------------|
| Variable-length string | Iceberg v2 | `string` → `string`<br>_TBD_ → `string`  | `string` → `varchar`<br>_TBD_            |
| Variable-length string | Hive (HMS) | _TBD_<br>_TBD_                           | `string` → `varchar`<br>_TBD_            |
| `VARCHAR(N)`           | Iceberg v2 | —<br>`VARCHAR(100)` → `string` ⚠         | —<br>_TBD_                               |
| `VARCHAR(N)`           | Hive (HMS) | _TBD_<br>`VARCHAR(100)` → `varchar(100)` | `varchar(100)` → `varchar(100)`<br>_TBD_ |
| `CHAR(N)`              | Iceberg v2 | —<br>`CHAR(10)` → `string` ⚠             | —<br>_TBD_                               |
| `CHAR(N)`              | Hive (HMS) | _TBD_<br>`CHAR(10)` → `char(10)`         | `char(10)` → `char(10)`<br>_TBD_         |

## Temporal

| Logical type           | Storage    | Spark                                     | Trino          |
|------------------------|------------|-------------------------------------------|----------------|
| `TIMESTAMP` (TZ-aware) | Iceberg v2 | _TBD_<br>`TIMESTAMP` → `timestamptz(6)` ⚠ | _TBD_<br>_TBD_ |
| `TIMESTAMP`            | Hive (HMS) | _TBD_<br>`TIMESTAMP` → `timestamp`        | _TBD_<br>_TBD_ |

## Binary and logical overlays

| Logical type    | Storage    | Spark                                   | Trino                                |
|-----------------|------------|-----------------------------------------|--------------------------------------|
| Variable binary | Iceberg v2 | `binary` → `binary`<br>_TBD_ → `binary` | `binary` → `varbinary`<br>_TBD_      |
| Variable binary | Hive (HMS) | _TBD_<br>_TBD_                          | `binary` → `varbinary`<br>_TBD_      |
| `fixed(N)`      | Iceberg v2 | `fixed(16)` → `binary` ⚠<br>_TBD_       | `fixed(16)` → `varbinary` ⚠<br>_TBD_ |
| `fixed(N)`      | Hive (HMS) | —<br>—                                  | —<br>—                               |
| `uuid`          | Iceberg v2 | _TBD_<br>_TBD_                          | `uuid` → `uuid`<br>_TBD_             |
| `uuid`          | Hive (HMS) | —<br>—                                  | —<br>—                               |

## Containers

| Logical type | Storage | Spark | Trino |
|---|---|---|---|
| `ARRAY<T>` | Iceberg v2 | `list<element:optional, string>` → `array<element-nullable:yes, string>`<br>`ARRAY<STRING>` → `list<element:optional, string>` | _TBD_<br>_TBD_ |
| `ARRAY<T>` | Hive (HMS) | `array<string>` → `array<element-nullable:yes, string>`<br>`ARRAY<STRING>` → `array<string>` | _TBD_<br>_TBD_ |
| `MAP<K, V>` | Iceberg v2 | `map<key:required, string; value:optional, int>` → `map<string, value-nullable:yes, int>`<br>`MAP<STRING, INT>` → `map<key:required, string; value:optional, int>` | _TBD_<br>_TBD_ |
| `MAP<K, V>` | Hive (HMS) | `map<string, int>` → `map<string, value-nullable:yes, int>`<br>`MAP<STRING, INT>` → `map<string, int>` | _TBD_<br>_TBD_ |
| `STRUCT<…>` | Iceberg v2 | `struct<field:required, value, int; field:optional, label, string>` → `struct<nullable:yes, value, int; nullable:yes, label, string>` ⚠<br>`STRUCT<value:INT, label:STRING>` → `struct<field:optional, value, int; field:optional, label, string>` | _TBD_<br>_TBD_ |
| `STRUCT<…>` | Hive (HMS) | `struct<value:int, label:string>` → `struct<nullable:yes, value, int; nullable:yes, label, string>`<br>`STRUCT<value:INT, label:STRING>` → `struct<value:int, label:string>` | _TBD_<br>_TBD_ |

## Union

| Logical type             | Storage    | Spark                                                                                                                      | Trino      |
|--------------------------|------------|----------------------------------------------------------------------------------------------------------------------------|------------|
| `UNIONTYPE<int>`         | Iceberg v2 | —<br>—                                                                                                                     | —<br>—     |
| `UNIONTYPE<int>`         | Hive (HMS) | `uniontype<int>` → `int` ⚠<br>—                                                                                            | _TBD_<br>— |
| `UNIONTYPE<int, string>` | Iceberg v2 | —<br>—                                                                                                                     | —<br>—     |
| `UNIONTYPE<int, string>` | Hive (HMS) | `uniontype<int, string>` → `struct<nullable:no, tag, int; nullable:yes, field0, int; nullable:yes, field1, string>` ⚠<br>— | _TBD_<br>— |
