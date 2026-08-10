# OpenHouse Spark integration

Two runtime artifacts are published from this directory:

| Module | Artifact | Spark |
| --- | --- | --- |
| `spark-3.1/openhouse-spark-runtime` | `openhouse-spark-runtime_2.12` | 3.1 |
| `spark-3.5/openhouse-spark-runtime` | `openhouse-spark-3.5-runtime_2.12` | 3.5 |

Both add the OpenHouse SQL extensions (`GRANT`/`REVOKE`/`SHOW GRANTS` and the `ALTER TABLE ... SET
POLICY` retention, replication, sharing, history and column-tag statements) to a Spark session. The
3.5 artifact repackages the 3.1 shadow jar, so **everything under `com.linkedin.openhouse.spark` is
compiled once against Spark 3.1 and executed on both releases.**

## SQL lineage

`com.linkedin.openhouse.spark.lineage` extracts table- and column-level lineage from the analyzed
logical plan of every statement, and answers four questions per statement:

* **which tables were read** — `inputTables`, including tables reached only through subqueries, CTEs
  and the target of a `MERGE`/`UPDATE`;
* **which table was written** — `outputTable`;
* **how each output column is calculated** — `columnLineage`, giving the upstream columns, the SQL
  formula and a coarse `transformationType` (`IDENTITY`, `LITERAL`, `EXPRESSION`, `AGGREGATION`,
  `WINDOW`, `GENERATOR`);
* **which columns decided the rows** — `conditions`, the indirect ("influence") lineage carried by
  `WHERE`, `JOIN ... ON`, `GROUP BY` and `MERGE ... ON`.

### Enabling it

Declaratively, as a query execution listener:

```
--conf spark.sql.queryExecutionListeners=com.linkedin.openhouse.spark.lineage.OpenhouseLineageListener
```

Every executed statement then produces one INFO line on the driver:

```
openhouse-lineage {"operation":"INSERT_INTO","sql":"INSERT INTO ...","outputTable":"openhouse.db.order_facts", ...}
```

Set the `com.linkedin.openhouse.spark.lineage` logger to `DEBUG` to additionally get a readable
block:

```
OpenHouse lineage
  operation   : INSERT_INTO
  outputTable : openhouse.db.order_facts
  inputTables : openhouse.db.orders, openhouse.db.customers
  columns     :
      openhouse.db.order_facts.revenue <- openhouse.db.orders.quantity, openhouse.db.orders.unit_price  [EXPRESSION] (quantity * unit_price)
      openhouse.db.order_facts.region <- openhouse.db.orders.region  [IDENTITY] region
  conditions  :
      FILTER: (tier = 'GOLD') -> openhouse.db.customers.tier
      JOIN: (customer_id = customer_id) -> openhouse.db.orders.customer_id, openhouse.db.customers.customer_id
```

Programmatically, which is what a shell session or a test wants:

```scala
val sink = new InMemoryLineageSink
OpenhouseLineageListener.register(spark, sink)
spark.sql("INSERT INTO openhouse.db.order_facts SELECT ... ").collect()
sink.last.foreach(l => println(l.toPrettyString))
```

Or without executing anything at all:

```scala
SqlLineageExtractor.extractFromSql(spark, "INSERT INTO db.t2 SELECT a * b AS c FROM db.t1")
```

### Emitting lineage somewhere other than the log

`LineageSink` is the only transport seam:

```scala
trait LineageSink { def emit(lineage: SqlLineage): Unit }
```

`LogLineageSink` (the default) writes to the driver log and `InMemoryLineageSink` collects in
memory. A Kafka publisher is a third implementation — `SqlLineage.toJson` already produces a compact
single-line payload — and is wired in by registering the listener programmatically with that sink.
Failures inside a sink are caught and logged: lineage capture never breaks the query that produced
it.

### What the tests demonstrate

`SqlLineageExtractorTest` in the 3.1 module is the catalogue of supported SQL shapes; each test names
the shape and the lineage it yields (projections, computed columns, `CASE`, aggregates, window
functions, joins, self joins, unions, CTEs, subqueries, `CTAS`, `INSERT`/`INSERT OVERWRITE`,
`MERGE`, `UPDATE`, `DELETE`, and the statements that intentionally produce no lineage).
`SqlLineageExtractorTestSpark3_5` in the 3.5 module reruns a representative subset on Spark 3.5.

Run them with a JDK 11 toolchain:

```bash
./gradlew :integrations:spark:spark-3.1:openhouse-spark-runtime_2.12:test --tests '*lineage*'
./gradlew :integrations:spark:spark-3.5:openhouse-spark-3.5-runtime_2.12:test
```

### Cross-version constraints

Because the code is compiled against Spark 3.1 and also runs on Spark 3.5, plan nodes are **never**
matched with case-class patterns — a fixed-arity `unapply` would throw `NoSuchMethodError` on the
release where the node gained a field (`CreateTableAsSelect`, `MergeIntoTable` and
`InsertIntoStatement` all did). `PlanAccessors` reads nodes through their accessor methods, whose
names and return types are stable, and node types are recognised by simple name. The same mechanism
transparently covers engine-specific nodes such as Iceberg's rewritten row-level plans, which are not
on the compile classpath at all.

Two Spark 3.4+ analysis changes are handled explicitly and are visible in the extracted lineage:

* CTEs are no longer inlined; `CTERelationRef` placeholders are linked back to their
  `CTERelationDef` so lineage still reaches the base tables.
* `UPDATE`, `DELETE` and `MERGE INTO` are rewritten into a generic `ReplaceData`/`WriteDelta` before
  the listener sees them. The original statement is recovered from the connector's
  `RowLevelOperation.command`, the internal row-tracking columns (`_file`, `_pos`) the rewrite adds
  are dropped, and the per-branch `MERGE` assignments are folded back into one entry per target
  column. Note that under copy-on-write an `UPDATE` genuinely rewrites every column of the matching
  files, so all of them are reported with the predicate column as a source.
