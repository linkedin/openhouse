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

// Pins on the physical form of what the OSS build writes. A case here fixes an implementation
// detail of the shipped write path, so a change to that detail shows up as a failing case. The
// behavior a case pins is an artifact of how OSS is wired, not a documented product feature.
trait ImplementationPinScenarios extends ScenarioKit {
  import Rows._

  // OpenHouse delegates table-data encryption to an external KMS plugin. The OSS build never wires
  // a KeyManagementClient into the catalog, so customer tables use the default
  // PlaintextEncryptionManager and data is written unencrypted. A Parquet file's footer magic bytes
  // are "PAR1" when unencrypted and "PARE" under modular encryption regardless of compression, so
  // this case checks that magic value to confirm the OSS write path produces plaintext data files.
  // An off-the-shelf KMS plugin alone would not change this result, because nothing in the
  // OpenHouse write path invokes the encryption hook without that wiring.
  lazy val encryptionPinCases: List[Plan.Case] = {
    val preparation = TablePreparation(
      "parquet",
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES ('write.format.default'='parquet')")()
        .insert(3)(),
      description = "Three seed rows in a parquet table.")

    List(
      preparation.test(
        "surface.pin.dataPlaintext",
        "A data file's Parquet footer magic bytes are the unencrypted PAR1 marker, confirming " +
          "OSS writes table data in plaintext.") { table =>
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
      })
  }
}
