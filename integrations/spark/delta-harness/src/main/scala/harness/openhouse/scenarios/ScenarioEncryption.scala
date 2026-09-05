package harness

import java.nio.charset.StandardCharsets

import org.apache.iceberg.spark.Spark3Util

/**
 * Encryption surface: the catalog writes default plaintext data files with the format's ordinary magic bytes.
 *
 * Operations: load the table's Iceberg FileIO, open one data file through that FileIO, and compare its magic bytes
 * with the expected plaintext marker. Parquet is checked by its trailing PAR1 footer bytes. ORC is checked by its
 * leading ORC bytes. This verifies observable default plaintext because the embedded catalog exposes no encryption
 * setting whose physical preservation can be observed.
 *
 * Preparation axes: the standard seeded core table in each columnar format.
 *
 * Case families: one family contributing 2 cases.
 */
trait ScenarioEncryption extends GovernanceTableFixtures {

  /** The plaintext data-file case, one file format at a time. */
  lazy val encryptionCases: List[TestCase] =
    fileFormats.map(format => dataFilePlaintextCase(preparedStandardTable(format)))

  /** A data file carries the format's ordinary plaintext magic bytes. */
  private def dataFilePlaintextCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("encryption.dataFilePlaintext") { table =>
      val dataFilePath = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
        .collect()(0)
        .getString(0)
      val expectedMagic =
        preparation.label match {
          case "parquet" => "PAR1".getBytes(StandardCharsets.US_ASCII)
          case "orc"     => "ORC".getBytes(StandardCharsets.US_ASCII)
        }
      val inputFile = Spark3Util.loadIcebergTable(table.spark, table.name).io().newInputFile(dataFilePath)

      assert(
        inputFile.getLength >= expectedMagic.length,
        s"data file should be large enough to inspect: ${inputFile.getLength} bytes")

      val inputStream = inputFile.newStream()
      val actualMagic =
        try {
          if (preparation.label == "parquet") {
            inputStream.seek(inputFile.getLength - expectedMagic.length)
          } else {
            inputStream.seek(0L)
          }
          (0 until expectedMagic.length).map { _ =>
            val nextByte = inputStream.read()
            assert(nextByte >= 0, s"data file ended before ${preparation.label} magic bytes were read")
            nextByte.toByte
          }.toArray
        } finally {
          inputStream.close()
        }

      assert(
        actualMagic.toSeq == expectedMagic.toSeq,
        s"expected plaintext ${preparation.label} magic ${new String(expectedMagic, StandardCharsets.US_ASCII)}, " +
          s"got ${new String(actualMagic, StandardCharsets.US_ASCII)}")
    }

}
