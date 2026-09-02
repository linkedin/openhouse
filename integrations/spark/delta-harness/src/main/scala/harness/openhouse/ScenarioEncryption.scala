package harness

import java.nio.file.{Files, Paths}

/**
 * Encryption: the OSS build writes table data in plaintext, because OpenHouse delegates table-data encryption to an
 * external KMS plugin and the OSS build wires no KeyManagementClient into the catalog, leaving the default
 * PlaintextEncryptionManager in place.
 *
 * Operations: read the trailing footer magic bytes of one data file. A Parquet footer reads PAR1 for plaintext and
 * PARE under modular encryption regardless of compression, so that magic value settles which path wrote the file.
 *
 * Preparation axes: the standard seeded core table in Parquet, which is the format whose footer carries the marker.
 *
 * Case families: one family contributing 1 case.
 */
trait ScenarioEncryption extends ScenarioKit {

  /** The plaintext data-file case, on the standard seeded Parquet table. */
  lazy val encryptionCases: List[Plan.Case] =
    List(dataFilePlaintextCase(preparedStandardTable("parquet")))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** A data file's Parquet footer magic bytes are the plaintext PAR1 marker. */
  private def dataFilePlaintextCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("encryption.dataFilePlaintext") { table =>
      val dataFilePath = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.data_files LIMIT 1")
        .collect()(0)
        .getString(0)
        .stripPrefix("file:")
      val bytes = Files.readAllBytes(Paths.get(dataFilePath))

      assert(
        bytes.length >= 8,
        s"data file is too small to inspect: ${bytes.length} bytes")
      val footerMagic = new String(bytes.takeRight(4), "US-ASCII")
      assert(
        footerMagic == "PAR1",
        s"expected plaintext Parquet footer PAR1, got $footerMagic")
    }

}
