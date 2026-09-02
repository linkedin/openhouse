package harness

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.Row

import scala.util.control.NonFatal

/**
 * Concurrency: two writers racing on one table. Every write either commits or fails with a typed commit-conflict
 * exception, and the table the race leaves behind is consistent with the writes that committed.
 *
 * Operations: two threads each running three single-row INSERTs against the same table, and two threads each running
 * an UPDATE of the same row to a different value.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats.
 *
 * Concurrency mechanics come from `ConcurrencySupport`: it runs the racing functions on their own threads and reports
 * a thread that failed outside the operation under test, and it recognizes a typed commit conflict anywhere in a
 * cause chain. This layer keeps its own outcome assertions and reuses those two primitives.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioConcurrency extends ScenarioKit {

  /** Every concurrency case, one file format at a time. */
  lazy val concurrencyCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        appendAppendCase(preparedStandardTable(format)),
        updateUpdateCase(preparedStandardTable(format)))
    }

  // --- the preparations and case bodies the surface above composes ---

  /**
   * Two threads concurrently insert three distinct rows each. Each appended value records whether its INSERT
   * committed or hit a typed commit conflict, and an untyped failure is recorded as such. The race is consistent when
   * at least one append commits and no append fails with an untyped error, and the table then holds exactly the three
   * seed rows plus the rows whose appends committed.
   */
  private def appendAppendCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("concurrency.appendAppend") { table =>
      val committedOutcome = "committed"
      val conflictedOutcome = "conflicted"
      val outcomeByValue = new ConcurrentHashMap[Int, String]()
      def writer(base: Int): () => Unit = () =>
        (0 until 3).foreach { offset =>
          val value = base + offset
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                s"(CAST($value AS BIGINT), $value, 'row-c', 1.5, true, '2024-01-09-01')")
            outcomeByValue.put(value, committedOutcome)
          } catch {
            case NonFatal(conflict) if ConcurrencySupport.isTypedCommitConflict(conflict) =>
              outcomeByValue.put(value, conflictedOutcome)
            case NonFatal(failure) =>
              outcomeByValue.put(value, s"untyped:${failure.getClass.getName}")
          }
        }

      val threadErrors = ConcurrencySupport.runConcurrently(Seq(writer(100), writer(200)))
      assert(
        threadErrors.isEmpty,
        s"a writer thread failed outside its INSERT loop: $threadErrors")

      val appendedValues = List(100, 101, 102, 200, 201, 202)
      val recordedOutcomes = appendedValues.map(value => value -> Option(outcomeByValue.get(value)))
      assert(
        recordedOutcomes.forall { case (_, outcome) => outcome.isDefined },
        s"every appended value records an outcome, found $recordedOutcomes")
      assert(
        recordedOutcomes.forall { case (_, outcome) => !outcome.exists(_.startsWith("untyped:")) },
        s"an append failed with an untyped error: $recordedOutcomes")

      val committedValues = recordedOutcomes.collect {
        case (value, Some(`committedOutcome`)) => value
      }
      assert(
        committedValues.nonEmpty,
        s"at least one append must commit, found $recordedOutcomes")

      val expectedRows = inKeyOrder(
        table.preparedRows ++
          committedValues.map(value => Row(value.toLong, value, "row-c", 1.5d, true, "2024-01-09-01")))
      assert(
        table.rows == expectedRows,
        s"the table holds the seed plus exactly the committed appends $committedValues, found ${table.rows}")
    }

  /**
   * Two threads concurrently UPDATE the same row to different values. Each updater records whether its statement
   * committed or hit a typed commit conflict, and an untyped failure is recorded as such. The race is consistent when
   * at least one update commits, no updater fails with an untyped error, and neither updater is left only conflicted.
   * The surviving value in row 2 is the value written by a committed updater, and every other row keeps its seed
   * value, so the table holds exactly the three seed rows with row 2 updated to the winner.
   */
  private def updateUpdateCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("concurrency.updateUpdate") { table =>
      val committedOutcome = "committed"
      val conflictedOutcome = "conflicted"
      val outcomeByWriter = new ConcurrentHashMap[String, String]()
      def updater(value: String): () => Unit = () =>
        try {
          table.spark.sql(
            s"UPDATE ${table.name} SET ${Core.string0.columnName} = '$value' " +
              s"WHERE ${Core.long0.columnName} = 2")
          outcomeByWriter.put(value, committedOutcome)
        } catch {
          case NonFatal(conflict) if ConcurrencySupport.isTypedCommitConflict(conflict) =>
            outcomeByWriter.put(value, conflictedOutcome)
          case NonFatal(failure) =>
            outcomeByWriter.put(value, s"untyped:${failure.getClass.getName}")
        }

      val threadErrors = ConcurrencySupport.runConcurrently(Seq(updater("AAA"), updater("BBB")))
      assert(
        threadErrors.isEmpty,
        s"an updater thread failed outside its UPDATE: $threadErrors")

      val recordedOutcomes = List("AAA", "BBB").map(value => value -> Option(outcomeByWriter.get(value)))
      assert(
        recordedOutcomes.forall { case (_, outcome) => outcome.isDefined },
        s"both updaters record an outcome, found $recordedOutcomes")
      assert(
        recordedOutcomes.forall { case (_, outcome) => !outcome.exists(_.startsWith("untyped:")) },
        s"an updater failed with an untyped error: $recordedOutcomes")

      val committedValues = recordedOutcomes.collect {
        case (value, Some(`committedOutcome`)) => value
      }
      assert(
        committedValues.nonEmpty,
        s"at least one update must commit rather than both conflicting, found $recordedOutcomes")

      val finalValue = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 2")
        .collect()(0)
        .getString(0)
      assert(
        committedValues.contains(finalValue),
        s"the surviving value is a committed writer's, found $finalValue with committed=$committedValues")

      val expectedRows = inKeyOrder(table.preparedRows.map { row =>
        if (Rows.TypedRow(row).get(Core.long0) == 2L) {
          withColumnValue(row, Core.string0, finalValue)
        } else {
          row
        }
      })
      assert(
        table.rows == expectedRows,
        s"the table holds the three seed rows with row 2 updated to $finalValue, found ${table.rows}")
    }

}
