package harness

import java.util.{Map => JavaMap}

import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import scala.util.Try

/**
 * File replication: the output-file property the writer stamps so the file system can set a block replication factor
 * on files a commit produces.
 *
 * Operations: read OutputFileFactory.FILE_REPLICATION_FACTOR, build an OutputFileFactory carrying a replication
 * factor, read the property map that factory stamps onto its output files, and write to the table afterwards.
 *
 * Preparation axes: one format-version-2 table built inside the case, because the case needs an Iceberg Table handle
 * to build a factory from.
 *
 * Case families: one family contributing 1 case.
 */
trait ScenarioFileReplication extends ScenarioKit {

  /** The output-file replication property case. */
  lazy val fileReplicationCases: List[TestCase] =
    List(TestCase("fileReplication.outputFileProperty @ core", outputFilePropertyCase))

  /**
   * OutputFileFactory exposes FILE_REPLICATION_FACTOR as "file-replication-factor", and a factory built with a
   * replication factor stamps that key into the property map of the output files it creates. Writes made through the
   * table afterwards still return the correct rows. The key is the one HDFS reads to set block replication on an
   * output file when a replication factor is supplied to the factory, and the delete-file write path is the one path
   * that supplies one. Reflection reaches the builder and getProperties because some Iceberg artifacts leave them out
   * of the public compiled API, so a direct reference would fail to compile against those artifacts.
   */
  private def outputFilePropertyCase(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val outputFileFactoryClass = Class.forName("org.apache.iceberg.io.OutputFileFactory")

    val replicationKeyField = Try(outputFileFactoryClass.getField("FILE_REPLICATION_FACTOR"))
    assert(replicationKeyField.isSuccess, "OutputFileFactory.FILE_REPLICATION_FACTOR is absent")
    val replicationKey = replicationKeyField.get.get(null).asInstanceOf[String]
    assert(
      replicationKey == "file-replication-factor",
      s"""expected FILE_REPLICATION_FACTOR to equal "file-replication-factor", got "$replicationKey"""")

    val table = TableTest.nextQualifiedTableName(ctx.namespace)
    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
          "TBLPROPERTIES ('format-version'='2')")) {
      spark.sql(s"INSERT INTO $table VALUES (1,'a'),(2,'b')")
      val icebergTable = Spark3Util.loadIcebergTable(spark, table)
      val builder = outputFileFactoryClass
        .getMethod("builderFor", classOf[Table], classOf[Int], classOf[Long])
        .invoke(null, icebergTable, Int.box(1), Long.box(1L))
      val replicationFactorMethod =
        Try(builder.getClass.getMethod("replicationFactor", classOf[Short]))
      assert(
        replicationFactorMethod.isSuccess,
        "OutputFileFactory.Builder.replicationFactor(short) is absent")
      replicationFactorMethod.get.invoke(builder, Short.box(2.toShort))
      val factory = Option(builder.getClass.getMethod("build").invoke(builder))
        .getOrElse(throw new AssertionError("OutputFileFactory build returned null"))

      val getProperties = outputFileFactoryClass.getDeclaredMethod("getProperties")
      getProperties.setAccessible(true)
      val outputFileProperties =
        getProperties.invoke(factory).asInstanceOf[JavaMap[String, String]]
      assert(
        outputFileProperties.get(replicationKey) == "2",
        s"expected output-file property $replicationKey=2 stamped by the factory, " +
          s"got ${outputFileProperties.get(replicationKey)}")

      spark.sql(s"INSERT INTO $table VALUES (3,'c')")
      val keys = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
      assert(keys == Seq(1L, 2L, 3L), s"rows after write should be 1, 2 and 3: $keys")
    }
  }

}
