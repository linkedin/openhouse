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

// Pins on the physical form of what the OSS build writes. A case here fixes an implementation detail of the shipped
// write path, so a change to that detail shows up as a failing case. The behavior a case pins is an artifact of how OSS
// is wired, not a documented product feature.
trait ImplementationPinScenarios extends ScenarioKit {
  import Rows._

  /**
   * A data file's Parquet footer magic bytes are the plaintext PAR1 marker, confirming OSS writes table data in
   * plaintext. OpenHouse delegates table-data encryption to an external KMS plugin and the OSS build wires no
   * KeyManagementClient into the catalog, so tables use the default PlaintextEncryptionManager. A Parquet footer reads
   * PAR1 for plaintext and PARE under modular encryption regardless of compression, so that magic value settles which
   * path wrote the file.
   */
  private def surfacePinDataPlaintextCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("surface.pin.dataPlaintext") { table =>
      val dataFilePath = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.data_files LIMIT 1")
        .collect()(0)
        .getString(0)
        .stripPrefix("file:")
      val bytes = java.nio.file.Files.readAllBytes(
        java.nio.file.Paths.get(dataFilePath))

      assert(
        bytes.length >= 8,
        s"data file is too small to inspect: ${bytes.length} bytes")
      val footerMagic = new String(bytes.takeRight(4), "US-ASCII")
      assert(
        footerMagic == "PAR1",
        s"expected plaintext Parquet footer PAR1, got $footerMagic")
    }

  /** The encryption pin, starting from three seed rows in a parquet table. */
  lazy val encryptionPinCases: List[Plan.Case] = {
    val preparation = TablePreparation(
      "parquet",
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES ('write.format.default'='parquet')")()
        .insert(3)())

    List(
      surfacePinDataPlaintextCase(preparation))
  }
}
