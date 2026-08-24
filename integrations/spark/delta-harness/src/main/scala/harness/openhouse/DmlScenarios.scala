package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

trait DmlScenarios extends ScenarioKit {
  import Rows._

  val ddlConsumerCases: List[Plan.Case] =
    layouts
      .filter(layout =>
        layout.label.endsWith("/parquet") ||
          layout.label.endsWith("/orc"))
      .flatMap { layout =>
        val preparations = List(
          TablePreparation(
            layout.label,
            createAndSeed(layout, 3)
              .sql("ddl")(table => s"ALTER TABLE $table ADD COLUMN cc int")(),
            "ddlConsume:addColumn."),
          TablePreparation(
            layout.label,
            createAndSeed(layout, 3)
              .sql("ddl")(table =>
                s"ALTER TABLE $table ALTER COLUMN ${Core.int0.columnName} TYPE bigint")(),
            "ddlConsume:typeWiden."),
          TablePreparation(
            layout.label,
            createAndSeed(layout, 3)
              .sql("ddl")(table =>
                s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")(),
            "ddlConsume:writeOrder."),
          TablePreparation(
            layout.label,
            createAndSeed(layout, 3)
              .sql("ddl")(table =>
                s"ALTER TABLE $table SET TBLPROPERTIES " +
                  "('write.distribution-mode'='range')")(),
            "ddlConsume:distMode."))

        preparations.flatMap { preparation =>
          List(
            preparation.test("dmlWrite") { table =>
              table.spark.sql(
                s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1")

              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 4,
                "table is not writable after DDL")
            },
            preparation.test("dmlMutate") { table =>
              table.spark.sql(
                s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 2,
                "mutation failed after DDL")
            },
            preparation.test("timeTravel") { table =>
              val seedSnapshotId =
                snapshotIds(table.spark, table.name).head

              assert(
                table.spark
                  .sql(
                    s"SELECT count(*) FROM ${table.name} " +
                      s"VERSION AS OF $seedSnapshotId")
                  .collect()(0)
                  .getLong(0) == 3,
                "seed snapshot is not readable after DDL")
            },
            preparation.test("restore") { table =>
              val seedSnapshotId =
                snapshotIds(table.spark, table.name).head

              table.spark.sql(
                s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1")
              table.spark.sql(
                "CALL openhouse.system.rollback_to_snapshot(" +
                  s"'${catalogRelative(table.name)}', $seedSnapshotId)")

              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 3,
                "restore across DDL failed")
            },
            preparation.test("expire") { table =>
              table.spark.sql(
                s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1")
              table.spark.sql(
                "CALL openhouse.system.expire_snapshots(" +
                  s"table => '${catalogRelative(table.name)}', " +
                  "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
                  "retain_last => 1)")

              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 4,
                "table is unreadable after snapshot expiration")
            },
            preparation.test("branch") { table =>
              table.spark.sql(
                s"ALTER TABLE ${table.name} CREATE BRANCH cb")
              table.spark.sql(
                s"INSERT INTO ${table.name}.branch_cb " +
                  s"SELECT * FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1")

              assert(
                table.spark
                  .sql(
                    s"SELECT count(*) FROM ${table.name} VERSION AS OF 'cb'")
                  .collect()(0)
                  .getLong(0) == 4,
                "branch write failed after DDL")
              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 3,
                "branch write changed the main table")
            },
            preparation.test("compact") { table =>
              table.spark.sql(
                s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1")
              table.spark.sql(
                "CALL openhouse.system.rewrite_data_files(" +
                  s"table => '${catalogRelative(table.name)}', " +
                  "options => map('min-input-files', '2'))")

              assert(
                table.spark
                  .sql(s"SELECT count(*) FROM ${table.name}")
                  .collect()(0)
                  .getLong(0) == 4,
                "compaction changed rows after DDL")
            })
        }
      }

  val createSchemaCases: List[Plan.Case] = preparedEmptyCoreTables.map { preparation =>
    preparation.test("create.schema") { table =>
      val actual = table.spark
        .table(table.name)
        .schema
        .fields
        .toList
        .map(field => field.name -> field.dataType.simpleString)
      val expected = Core.tableColumns.toList.map(column => (column.columnName, column.sqlType))

      assert(actual == expected)
      assert(table.rows.isEmpty)
    }
  }

  val ddlSchemaCases: List[Plan.Case] = preparedCoreTables.flatMap { preparation =>
    List(
      preparation.test("ddl.addColumn.single") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_int int")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)
        val nullCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name} WHERE added_int IS NULL")
          .collect()(0)
          .getLong(0)

        assert(columnNames.contains("added_int"), s"added_int missing: $columnNames")
        assert(
          nullCount == table.preparedRows.size,
          s"existing rows should read null for added_int: $nullCount != ${table.preparedRows.size}")
        assert(table.rows.size == table.preparedRows.size)
      },
      preparation.test("ddl.addColumn.multiple") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMNS (added_a int, added_b string)")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.contains("added_a") && columnNames.contains("added_b"),
          s"added columns missing: $columnNames")
        assert(table.rows.size == table.preparedRows.size)
      },
      preparation.test("ddl.addColumn.comment") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_c int COMMENT 'a note'")

        val addedColumn = table.spark
          .table(table.name)
          .schema
          .fields
          .find(_.name == "added_c")
          .getOrElse(throw new AssertionError("added_c missing"))

        assert(
          addedColumn.getComment().contains("a note"),
          s"comment not stored: ${addedColumn.getComment()}")
      },
      preparation.test("ddl.addColumn.position") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN added_after int AFTER ${Core.long0.columnName}")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.indexOf("added_after") == columnNames.indexOf(Core.long0.columnName) + 1,
          s"added_after not after long0: $columnNames")
      },
      preparation.test("ddl.alterColumn.typeWiden") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.int0.columnName} TYPE bigint")

        val liveColumns = table.spark.table(table.name).schema.fields.toSeq
          .map(field => field.name -> field.dataType.simpleString)
          .toMap
        val values = table.spark
          .sql(
            s"SELECT ${Core.int0.columnName} FROM ${table.name} ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.getLong(0))

        assert(
          liveColumns.get(Core.int0.columnName).contains("bigint"),
          s"int0 not widened: ${liveColumns.get(Core.int0.columnName)}")
        assert(values == Seq(1L, 2L, 3L), s"values not preserved after widening: $values")
      },
      preparation.test("ddl.renameColumn") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN to_rename int")
        table.spark.sql(s"ALTER TABLE ${table.name} RENAME COLUMN to_rename TO renamed_col")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.contains("renamed_col") && !columnNames.contains("to_rename"),
          s"RENAME COLUMN silently no-oped: $columnNames")
        assert(table.rows.size == table.preparedRows.size)
      })
  }

  private def localizedDmlCases(
      preparation: TablePreparation[CoreTable.type]
  ): List[Plan.Case] =
    List(
      preparation.test("read.projection") { table =>
        val expected = table.preparedRows
          .sortBy(_.get(Core.long0))
          .map(_.get(Core.string0))
        val actual = table.spark
          .sql(
            s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.string0))

        assert(actual == expected)
      },
      preparation.test("read.filter") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filter(_ >= 2)
          .sorted
        val actual = table.spark
          .sql(
            s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
              s"WHERE ${Core.long0.columnName} >= 2 ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.long0))

        assert(actual == expected)
      },
      preparation.test("format.materialization") { table =>
        val format = table.spark
          .sql(
            s"SHOW TBLPROPERTIES ${table.name} ('write.format.default')")
          .collect()(0)
          .getString(1)
        val filePaths = table.spark
          .sql(s"SELECT file_path FROM ${table.name}.files")
          .collect()
          .toSeq
          .map(_.getString(0))

        assert(
          filePaths.nonEmpty &&
            filePaths.forall(_.toLowerCase.endsWith(s".$format")),
          s"data files are not all .$format: $filePaths")
      },
      preparation.test("delete.byPredicate") { table =>
        val expected = table.preparedRows.filterNot(_.get(Core.long0) < 2)

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")

        assert(table.rows == expected)
      },
      preparation.test("delete.byInList") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(Set(1L, 3L))
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (1, 3)")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byInSubquery") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(_ == 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byNotInSubquery") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filter(_ == 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byExistsSubquery") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(_ == 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byNotExistsSubquery") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filter(_ == 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE NOT EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byScalarSubquery") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(_ == 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.byNullCondition") { table =>
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES (" +
            "CAST(99 AS BIGINT), 99, NULL, 99.5, false, '2024-01-01-00')")
        val rowsBeforeDelete = table.rows
        val expected = rowsBeforeDelete
          .filter(row => Option(row.get(Core.string0)).nonEmpty)
          .map(_.get(Core.long0))
          .sorted

        assert(
          rowsBeforeDelete.exists(row => Option(row.get(Core.string0)).isEmpty),
          "precondition: a null-string row was seeded")

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.string0.columnName} IS NULL")

        assert(keyed(table.rows) == expected)
        assert(!keyed(table.rows).contains(99L))
      },
      preparation.test("delete.all") { table =>
        table.spark.sql(s"DELETE FROM ${table.name}")

        assert(table.rows.isEmpty)
      },
      preparation.test("delete.none") { table =>
        val snapshotsBefore = table.snapshotCount

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 999")

        assert(table.rows == table.preparedRows)
        assert(
          table.snapshotCount == snapshotsBefore + 1,
          "no-match DELETE with a real predicate still commits one snapshot")
      },
      preparation.test("delete.byPartitionPredicate") { table =>
        val expected = table.preparedRows
          .filterNot(_.get(Core.datePartition) == "2024-01-01-00")
          .map(_.get(Core.long0))
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE " +
            s"${Core.datePartition.columnName} = '2024-01-01-00'")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.withAlias") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(_ < 2L)
          .sorted

        table.spark.sql(
          s"DELETE FROM ${table.name} AS x WHERE x.${Core.long0.columnName} < 2")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("delete.whereFalse.noSnapshot") { table =>
        val snapshotsBefore = table.snapshotCount

        table.spark.sql(s"DELETE FROM ${table.name} WHERE false")

        assert(table.rows == table.preparedRows)
        assert(
          table.snapshotCount == snapshotsBefore,
          "DELETE WHERE false must not commit a snapshot")
      },
      preparation.test("delete.truncate") { table =>
        table.spark.sql(s"TRUNCATE TABLE ${table.name}")

        assert(table.rows.isEmpty)
      },
      preparation.test("delete.atSnapshot.rejected") { table =>
        val snapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.snapshots " +
              "ORDER BY committed_at DESC LIMIT 1")
          .collect()(0)
          .getLong(0)
        val exception = Check.intercept[IllegalArgumentException](
          table.spark.sql(
            s"DELETE FROM ${table.name}.snapshot_id_$snapshotId " +
              s"WHERE ${Core.long0.columnName} < 4"))

        assert(
          exception.getMessage ==
            s"Cannot delete from table at a specific snapshot: $snapshotId")
        assert(table.rows == table.preparedRows)
      },
      preparation.test("update.byPredicate") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = 2")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.withoutCondition") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, _) => id -> "Z"
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Z'")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.noMatch") { table =>
        val snapshotsBefore = table.snapshotCount

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Y' " +
            s"WHERE ${Core.long0.columnName} = 99")

        assert(longToString(table.rows) == longToString(table.preparedRows))
        assert(
          table.snapshotCount == snapshotsBefore + 1,
          "no-match UPDATE still commits one snapshot")
      },
      preparation.test("update.byInSubquery") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.byNotInSubquery") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id != 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.byExistsSubquery") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.byNotExistsSubquery") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id != 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE NOT EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.byScalarSubquery") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.withAlias") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} AS x SET x.${Core.string0.columnName} = 'X' " +
            s"WHERE x.${Core.long0.columnName} = 2")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("update.multipleColumns") { table =>
        val expectedStrings = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "X" else value)
        }

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X', " +
            s"${Core.int0.columnName} = 99 WHERE ${Core.long0.columnName} = 2")

        assert(longToString(table.rows) == expectedStrings)
        assert(
          table.rows
            .find(_.get(Core.long0) == 2L)
            .map(_.get(Core.int0))
            .contains(99))
      },
      preparation.test("update.byExpression") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .map(value => if (value == 2L) 12L else value)
          .sorted

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.long0.columnName} = ${Core.long0.columnName} + 10 " +
            s"WHERE ${Core.long0.columnName} = 2")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("update.movePartition") { table =>
        val expected = table.preparedRows.map { row =>
          val id = row.get(Core.long0)
          id -> (if (id == 2) "2099-12-31-23" else row.get(Core.datePartition))
        }.toMap

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.datePartition.columnName} = '2099-12-31-23' " +
            s"WHERE ${Core.long0.columnName} = 2")

        val actual = table.rows.map(row =>
          row.get(Core.long0) -> row.get(Core.datePartition)).toMap

        assert(actual == expected)
      },
      preparation.test("update.nullAssignment") { table =>
        val expected = table.preparedRows.map { row =>
          val id = row.get(Core.long0)
          id -> (if (id == 2) None else Option(row.get(Core.string0)))
        }.toMap

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = NULL " +
            s"WHERE ${Core.long0.columnName} = 2")

        val actual = table.rows.map(row =>
          row.get(Core.long0) -> Option(row.get(Core.string0))).toMap

        assert(actual == expected)
      },
      preparation.test("merge.insertNotMatched") { table =>
        val expectedKeys =
          (table.preparedRows.map(_.get(Core.long0)) ++ Seq(4L, 5L)).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")

        assert(keyed(table.rows) == expectedKeys)
        assert(
          table.rows
            .find(_.get(Core.long0) == 4L)
            .map(_.get(Core.string0))
            .contains("row-4"))
        assert(
          table.rows
            .find(_.get(Core.long0) == 5L)
            .map(_.get(Core.string0))
            .contains("row-5"))
      },
      preparation.test("merge.updateMatched") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "M" else value)
        }

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("merge.deleteMatched") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(Set(1L, 3L))
          .sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN DELETE""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("merge.upsert") { table =>
        val updated = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "U" else value)
        }
        val expected =
          if (table.preparedRows.exists(_.get(Core.long0) == 7L)) updated
          else updated + (7L -> "g")

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'U', 2.5, true,  '2024-01-02-01'),
                  (CAST(7 AS BIGINT), 7, 'g', 7.5, false, '2024-01-07-06')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("merge.deleteNotMatchedBySource") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filter(_ == 2L)
          .sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("merge.conditionalUpdate") { table =>
        val expected = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "U2" else value)
        }

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U2'),
                  (CAST(3 AS BIGINT), 'U3')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")

        assert(longToString(table.rows) == expected)
      },
      preparation.test("merge.multipleMatchedClauses") { table =>
        val expected = table.preparedRows
          .map(_.get(Core.long0))
          .filterNot(_ == 3L)
          .sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U'),
                  (CAST(3 AS BIGINT), 'x')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN MATCHED THEN DELETE""")

        assert(keyed(table.rows) == expected)
        assert(
          table.rows
            .find(_.get(Core.long0) == 2L)
            .map(_.get(Core.string0))
            .contains("U"))
      },
      preparation.test("merge.conditionalInsert") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 4L).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED AND s.${Core.long0.columnName} = 4 THEN INSERT *""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("merge.allClauses") { table =>
        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'M2', 2.5, true,  '2024-01-02-01'),
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, false, '2024-01-04-03')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")

        assert(keyed(table.rows) == Seq(2L, 4L))
        assert(
          table.rows
            .find(_.get(Core.long0) == 2L)
            .map(_.get(Core.string0))
            .contains("M2"))
      },
      preparation.test("merge.updateStar") { table =>
        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 22, 'S2', 22.5, true, '2024-06-06-06')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE SET *""")

        val updatedRow = table.rows.find(_.get(Core.long0) == 2L)

        assert(updatedRow.map(_.get(Core.string0)).contains("S2"))
        assert(updatedRow.map(_.get(Core.int0)).contains(22))
      },
      preparation.test("merge.insertExplicitColumns") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 7L).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(7 AS BIGINT), 'g')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}, ${Core.string0.columnName})
              VALUES (s.${Core.long0.columnName}, s.${Core.string0.columnName})""")

        assert(keyed(table.rows) == expected)
        assert(
          table.rows
            .find(_.get(Core.long0) == 7L)
            .map(_.get(Core.string0))
            .contains("g"))
      },
      preparation.test("merge.sourceCTE") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 8L).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                WITH src AS (
                  SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                )
                SELECT * FROM src
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("merge.sourceSetOp") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) ++ Seq(8L, 9L)).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                UNION ALL
                SELECT CAST(9 AS BIGINT)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("merge.intoEmptyTarget") { table =>
        table.spark.sql(s"DELETE FROM ${table.name}")
        assert(table.rows.isEmpty)

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")

        assert(keyed(table.rows) == Seq(4L, 5L))
      },
      preparation.test("merge.nullJoinKey") { table =>
        val expectedStrings = longToString(table.preparedRows).map {
          case (id, value) => id -> (if (id == 2) "M" else value)
        }

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(NULL AS BIGINT), 'n'),
                  (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")

        assert(
          keyed(table.rows) ==
            table.preparedRows.map(_.get(Core.long0)).sorted)
        assert(longToString(table.rows) == expectedStrings)
      },
      preparation.test("merge.resolveByName") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 7L).sorted

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  ('g', CAST(7 AS BIGINT), 7, 7.5, false, '2024-07-07-07')
                AS s(
                  ${Core.string0.columnName},
                  ${Core.long0.columnName},
                  ${Core.int0.columnName},
                  ${Core.double0.columnName},
                  ${Core.boolean0.columnName},
                  datepartition)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")

        assert(keyed(table.rows) == expected)
        assert(
          table.rows
            .find(_.get(Core.long0) == 7L)
            .map(_.get(Core.string0))
            .contains("g"))
      },
      preparation.test("insert.into") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) ++ Seq(4L, 5L)).sorted

        table.spark.sql(
          s"""INSERT INTO ${table.name} VALUES
                (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')""")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("insert.explicitColumns") { table =>
        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"INSERT INTO ${table.name} " +
              s"(${Core.long0.columnName}, ${Core.string0.columnName}) " +
              "VALUES (CAST(4 AS BIGINT), 'd'), (CAST(5 AS BIGINT), 'e')"))
        val exceptionMessage =
          Option(exception.getMessage).getOrElse("")

        assert(
          exceptionMessage.toUpperCase.contains("CANNOT_FIND_DATA") ||
            exceptionMessage.toUpperCase.contains("CANNOT FIND DATA") ||
            exceptionMessage.toUpperCase.contains("INCOMPATIBLE_DATA"),
          "expected a partial-INSERT rejection naming the omitted column " +
            s"(engine limitation), got: ${exceptionMessage.take(200)}")
      },
      preparation.test("insert.intoSelect") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 6L).sorted

        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM VALUES " +
            s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
            s"AS s($cols)")

        assert(keyed(table.rows) == expected)
      },
      preparation.test("append.dataFrame") { table =>
        val expected =
          (table.preparedRows.map(_.get(Core.long0)) :+ 6L).sorted
        val frame = table.spark.sql(
          s"SELECT * FROM VALUES " +
            s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
            s"AS s($cols)")

        frame.writeTo(table.name).append()

        assert(keyed(table.rows) == expected)
      },
      preparation.test("insert.overwrite") { table =>
        table.spark.sql(
          s"""INSERT OVERWRITE ${table.name} VALUES
                (CAST(1 AS BIGINT), 1, 'p', 1.5, false, '2024-01-01-00'),
                (CAST(2 AS BIGINT), 2, 'q', 2.5, true,  '2024-01-02-01')""")

        assert(keyed(table.rows) == Seq(1L, 2L))
      },
      preparation.test("overwrite.dataFrame") { table =>
        val frame = table.spark.sql(
          s"SELECT * FROM VALUES " +
            s"(CAST(8 AS BIGINT), 8, 'h', 8.5, false, '2024-01-08-07') " +
            s"AS s($cols)")

        frame.writeTo(table.name).overwrite(
          org.apache.spark.sql.functions.lit(true))
        assert(keyed(table.rows) == Seq(8L))
      })

  private def operationName(
      testCase: Plan.Case,
      preparation: TablePreparation[CoreTable.type]
  ): String =
    testCase.id
      .split(" @ ", 2)
      .head
      .stripPrefix(preparation.casePrefix)

  private def localizedMutationDmlCases(
      preparation: TablePreparation[CoreTable.type]
  ): List[Plan.Case] =
    localizedDmlCases(preparation).filter { testCase =>
      val caseName = operationName(testCase, preparation)
      caseName.startsWith("delete.") ||
        caseName.startsWith("update.") ||
        caseName.startsWith("merge.")
    }

  val coreDmlCases: List[Plan.Case] =
    preparedCoreTables.flatMap(localizedDmlCases)

  val morDmlCases: List[Plan.Case] =
    preparedMorCoreTables.flatMap(localizedMutationDmlCases)

  val orderedDmlCases: List[Plan.Case] =
    preparedOrderedCoreTables.flatMap(localizedDmlCases)

  val evolvedDmlCases: List[Plan.Case] =
    preparedEvolvedCoreTables.flatMap { preparation =>
      localizedDmlCases(preparation).filter { testCase =>
        val caseName = operationName(testCase, preparation)
        (caseName.startsWith("delete.") ||
          caseName.startsWith("update.") ||
          caseName.startsWith("read.")) &&
          !caseName.contains("byNullCondition")
      }
    }

  val rtasDmlCases: List[Plan.Case] =
    preparedRtasCoreTables.flatMap(localizedDmlCases)

  val rtasMorDmlCases: List[Plan.Case] =
    preparedRtasMorCoreTables.flatMap(localizedMutationDmlCases)

  val branchDmlCases: List[Plan.Case] =
    preparedBranchCoreTables.flatMap(localizedDmlCases)

  val branchMorDmlCases: List[Plan.Case] =
    preparedBranchMorCoreTables.flatMap(localizedMutationDmlCases)

  val morReadDmlCases: List[Plan.Case] =
    preparedMorReadCoreTables.flatMap { preparation =>
      localizedDmlCases(preparation).filter { testCase =>
        val caseName = operationName(testCase, preparation)
        caseName.startsWith("read.") ||
          caseName == "format.materialization"
      }
    }

  def undroppedDmlCases: List[Plan.Case] =
    if (HtsAdmin.enabled) preparedUndroppedCoreTables.flatMap(localizedDmlCases)
    else Nil

  private def localizedPartitionedDmlCases(
      preparation: TablePreparation[CoreTable.type]
  ): List[Plan.Case] =
    List(
      preparation.test("insert.dynamicOverwrite") { table =>
        val expected =
          (table.preparedRows
            .filterNot(_.get(Core.datePartition) == "2024-01-01-00")
            .map(_.get(Core.long0)) :+ 10L).sorted

        table.spark.conf.set(
          "spark.sql.sources.partitionOverwriteMode",
          "dynamic")
        try {
          table.spark.sql(
            s"INSERT OVERWRITE ${table.name} VALUES " +
              "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00')")
        } finally {
          table.spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "static")
        }

        assert(keyed(table.rows) == expected)
      },
      preparation.test("overwrite.partitions") { table =>
        val expected =
          (table.preparedRows
            .filterNot(_.get(Core.datePartition) == "2024-01-01-00")
            .map(_.get(Core.long0)) :+ 10L).sorted
        val frame = table.spark.sql(
          s"SELECT * FROM VALUES " +
            "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00') " +
            s"AS s($cols)")

        frame.writeTo(table.name).overwritePartitions()

        assert(keyed(table.rows) == expected)
      })

  val partitionedDmlCases: List[Plan.Case] =
    preparedCoreTables
      .filter(_.label.startsWith("partitioned/"))
      .flatMap(localizedPartitionedDmlCases)

  val rtasPartitionedDmlCases: List[Plan.Case] =
    preparedRtasCoreTables
      .filter(_.label.startsWith("partitioned/"))
      .flatMap(localizedPartitionedDmlCases)

  val branchPartitionedDmlCases: List[Plan.Case] =
    preparedBranchCoreTables
      .filter(_.label.startsWith("partitioned/"))
      .flatMap(localizedPartitionedDmlCases)

  // ── MoR discriminator: prove merge-on-read actually wrote position-delete files ──────────
  // The rest of the MoR axis reuses CoW's row-delta assertions, which pass identically whether the
  // write was copy-on-write or merge-on-read. These two pin the PHYSICAL difference: a MoR delete
  // MUST add a position-delete file; a CoW delete must NOT. Both are prepared with
  // `createAndSeedSingleFile` and delete a strict subset (`long0 < 2` → 1 of 3 rows), so the write
  // cannot be satisfied by whole-file elimination — the outcome is deterministic across formats
  // (verified: parquet/orc/avro all add exactly one position delete under MoR, none under CoW).
  private def deleteFileCount(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)

  val deleteFileModeCases: List[Plan.Case] = {
    def cases(
        layouts: List[Layout],
        caseName: String,
        expectDeleteFiles: Boolean): List[Plan.Case] =
      layouts.map { layout =>
        val preparation = TablePreparation(
          layout.label,
          createAndSeedSingleFile(layout, 3))
        preparation.test(caseName) { table =>
          val rowsBefore = table.rows
          table.spark.sql(
            s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
          val rowsAfter = table.rows
          val deleteFileCountAfter =
            deleteFileCount(table.spark, table.name)

          assert(
            rowsAfter == rowsBefore.filterNot(_.get(Core.long0) < 2),
            "strict-subset DELETE returned an unexpected row set")
          if (expectDeleteFiles) {
            assert(
              deleteFileCountAfter >= 1,
              "merge-on-read DELETE should write a position-delete file")
          } else {
            assert(
              deleteFileCountAfter == 0,
              "copy-on-write DELETE should not write delete files")
          }
        }
      }

    cases(
      morVerifyLayouts,
      "mor.writesDeleteFiles",
      expectDeleteFiles = true) ++
      cases(
        cowVerifyLayouts,
        "cow.writesNoDeleteFiles",
        expectDeleteFiles = false)
  }


}
