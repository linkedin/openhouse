package harness

import java.lang.reflect.Method
import java.nio.file.{Files, Path}
import java.util.{ArrayList, Arrays, Comparator}

import org.apache.iceberg.{HasTableOperations, SchemaParser, TableMetadata}

// `harness.Schema` is the harness's own table-shape trait, so the Iceberg schema type is aliased here.
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.types.Types
import org.apache.spark.sql.AnalysisException

import scala.collection.JavaConverters._

/**
 * Column defaults: what happens to a DEFAULT a caller attaches to a column, from the SQL surface that parses it down
 * to the Iceberg schema that would have to carry it.
 *
 * Operations: ALTER TABLE ADD COLUMN ... DEFAULT followed by a DESCRIBE of the column, a read of the pre-existing
 * rows, and an INSERT that omits the column; serializing a NestedField that carries an initial default and round
 * tripping it through SchemaParser; and committing a defaulted column through the low-level TableMetadata API over a
 * table whose data files were written before the column existed.
 *
 * Preparation axes: each case builds what it needs. The ADD COLUMN family builds a two-column table in each of the two
 * columnar formats. The serialization family runs against Iceberg schema objects directly. The read-apply probe builds
 * its own Hadoop catalog over a temporary warehouse because its commit uses the low-level metadata surface.
 *
 * Case families: three families contributing 4 cases.
 */
trait ScenarioColumnDefault extends ColumnDefaultTableFixtures {

  /** Every column-default case: the two ADD COLUMN formats, then the API serialization and read-apply probes. */
  lazy val columnDefaultCases: List[TestCase] =
    fileFormats.map(format =>
      TestCase(s"columnDefault.addColumnInert @ $format", addColumnInertCase(format))) ++
      List(
        TestCase("columnDefault.apiSerialization @ core", _ => apiSerialization()),
        TestCase("columnDefault.readApplyProbe @ core", readApplyProbe))

  // --- the case bodies and the reflective field builder the surface above composes ---

  // Reflection supports Iceberg artifacts across NestedField.builder API versions. The lookup runs only when a case
  // executes, keeping catalog construction independent of that API version.
  private def nestedFieldClass: Class[_] =
    Class.forName("org.apache.iceberg.types.Types$NestedField")

  private def nestedFieldBuilder: Option[Method] =
    try {
      Some(nestedFieldClass.getMethod("builder"))
    } catch {
      case _: NoSuchMethodException => None
    }

  private def invokeWithArgument(
      target: AnyRef,
      methodName: String,
      argumentType: Class[_],
      argument: AnyRef): AnyRef =
    target.getClass.getMethod(methodName, argumentType).invoke(target, argument)

  /**
   * Reflectively builds an `optional int` NestedField carrying `initialDefault`. The result identifies whether the
   * active Iceberg artifact exposes the builder API.
   */
  private def buildDefaultedIntegerField(
      fieldId: Int,
      columnName: String,
      initialDefault: Int): Option[Types.NestedField] =
    nestedFieldBuilder.map { builderFactory =>
      val integerType =
        Class.forName("org.apache.iceberg.types.Types$IntegerType").getMethod("get").invoke(null)
      val builderSteps: List[AnyRef => AnyRef] = List(
        builder => invokeWithArgument(builder, "withId", classOf[Int], Int.box(fieldId)),
        builder => invokeWithArgument(builder, "withName", classOf[String], columnName),
        builder =>
          invokeWithArgument(
            builder,
            "ofType",
            Class.forName("org.apache.iceberg.types.Type"),
            integerType),
        builder => builder.getClass.getMethod("asOptional").invoke(builder),
        builder =>
          invokeWithArgument(builder, "withInitialDefault", classOf[Object], Int.box(initialDefault)))
      val builder =
        builderSteps.foldLeft(builderFactory.invoke(null))((built, step) => step(built))

      builder.getClass.getMethod("build").invoke(builder).asInstanceOf[Types.NestedField]
    }

  /**
   * ALTER TABLE ADD COLUMN c int DEFAULT 5 parses, and the default value stops at the parser: the committed schema
   * records c as a plain optional column, pre-existing rows read null for it, and an INSERT that omits c is rejected
   * with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA. The file format is the parameter.
   */
  private def addColumnInertCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
          s"TBLPROPERTIES ('write.format.default'='$format')")) {
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")

      // The DDL is accepted at parse time; Spark owns the DEFAULT grammar.
      spark.sql(s"ALTER TABLE $table ADD COLUMN c int DEFAULT 5")

      // The committed schema describes c as a plain optional int, with the default left behind at the parser.
      val describedColumn = spark
        .sql(s"DESCRIBE TABLE EXTENDED $table")
        .collect()
        .toSeq
        .map(_.mkString("|"))
        .filter(_.matches("(?i)^c\\|.*"))
        .mkString(" ;; ")
      assert(
        describedColumn.nonEmpty,
        s"[$format] DESCRIBE TABLE EXTENDED must include the added column c")
      assert(
        !describedColumn.toLowerCase.contains("default") && !describedColumn.contains("5"),
        s"[$format] expected no default persisted for c, but DESCRIBE shows: $describedColumn")

      // The read path returns null for c on every row written before the column existed.
      val nullCount =
        spark.sql(s"SELECT count(*) FROM $table WHERE c IS NULL").collect()(0).getLong(0)
      assert(
        nullCount == 2,
        s"[$format] expected the default not applied on read (2 nulls), got $nullCount")

      // The write path requires a value for c, so an insert that omits it is rejected.
      val omittedColumnFailure = Check.intercept[AnalysisException](
        spark.sql(s"INSERT INTO $table (id, s) VALUES (3, 'c')"))
      val omittedColumnMessages = Exceptions
        .causeChain(omittedColumnFailure)
        .flatMap(failure => Option(failure.getMessage))
        .mkString(" | ")
      assert(
        omittedColumnMessages.contains("CANNOT_FIND_DATA"),
        s"[$format] expected omit-insert rejected with CANNOT_FIND_DATA, got: $omittedColumnMessages")

      println(
        s"DIAG columnDefault.addColumnInert[$format]: accepted=yes persistedDefault=absent " +
          "readBackfill=absent writeApply=rejected(CANNOT_FIND_DATA)")
    }
  }

  /**
   * A NestedField built with an initial default serializes initial-default into the schema JSON, and that value
   * survives a fromJson then toJson round trip. SchemaParser.toJson uses one artifact-wide serialization contract. An
   * artifact that lacks NestedField.builder also lacks the initialDefault and writeDefault accessors, and the case
   * pins that API shape.
   */
  private def apiSerialization(): Unit =
    buildDefaultedIntegerField(3, "c", 5) match {
      case None =>
        println(
          "DIAG columnDefault.apiSerialization: NestedField.builder absent, column-default API " +
            "unsupported on this artifact")
        val methodNames = nestedFieldClass.getMethods.map(_.getName).toSet
        assert(
          !methodNames.contains("initialDefault") && !methodNames.contains("writeDefault"),
          "NestedField builder and default accessors must be available as one API")

      case Some(defaultedField) =>
        // A schema of [id, c(default=5)], serialized through the artifact-wide SchemaParser contract.
        val idField = Types.NestedField.required(1, "id", Types.LongType.get())
        val schema = new IcebergSchema(Arrays.asList(idField, defaultedField))
        val json = SchemaParser.toJson(schema)
        println(
          s"DIAG columnDefault.apiSerialization: column-default API present, schema JSON = $json")

        assert(
          json.contains("initial-default"),
          s"expected SchemaParser to serialize 'initial-default' into the schema JSON, got: $json")
        val roundTrippedJson = SchemaParser.toJson(SchemaParser.fromJson(json))
        assert(
          roundTrippedJson.contains("initial-default"),
          s"expected 'initial-default' to survive the fromJson/toJson round trip, got: $roundTrippedJson")
        println(
          "DIAG columnDefault.apiSerialization: initial-default serialized and round-trips")
    }

  /**
   * A column default added after data files exist persists into the committed schema. The schema evolution goes
   * through the low-level TableMetadata API, which supplies the set-default operation, and through a Hadoop catalog
   * the case configures for itself. The documented read contract covers schema persistence, and the case records the
   * OSS Spark read result for pre-existing rows as diagnostic output.
   */
  private def readApplyProbe(ctx: Ctx): Unit = {
    val spark = ctx.spark

    if (nestedFieldBuilder.isEmpty) {
      // The builder and default accessors form one API surface, so the case pins their joint availability.
      println(
        "DIAG columnDefault.readApplyProbe: column-default builder API is absent")
      assert(
        !nestedFieldClass.getMethods.map(_.getName).toSet.contains("initialDefault"),
        "NestedField exposes initialDefault but builder() is absent")
    } else {
      // The probe catalog, its warehouse and its table all carry the same generated token, so two runs of this case
      // share nothing. The token comes from the harness table-name generator, which is UUID plus a counter.
      val probeName = TableTest.nextQualifiedTableName("d").split('.').last
      val probeCatalog = s"columnDefaultProbe_$probeName"
      val probeTable = s"$probeCatalog.d.$probeName"
      val warehouse = Files.createTempDirectory(probeName)

      OwnedTableLifecycle.withCleanup(deleteRecursively(warehouse)) {
        spark.conf.set(s"spark.sql.catalog.$probeCatalog", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(s"spark.sql.catalog.$probeCatalog.type", "hadoop")
        spark.conf.set(s"spark.sql.catalog.$probeCatalog.warehouse", warehouse.toString)

        withOwnedTable(spark.sql(_), probeTable)(
          spark.sql(s"CREATE TABLE $probeTable (id bigint) USING $dataSource")) {
          // The data files these rows produce physically carry the id column alone.
          spark.sql(s"INSERT INTO $probeTable VALUES (1),(2)")

          val icebergTable = Spark3Util.loadIcebergTable(spark, probeTable)
          val currentSchema = icebergTable.schema()
          val defaultedField =
            buildDefaultedIntegerField(currentSchema.highestFieldId() + 1, "c", 5)
              .getOrElse(throw new AssertionError("builder API present but field build failed"))
          val evolvedColumns = new ArrayList[Types.NestedField](currentSchema.columns())
          evolvedColumns.add(defaultedField)
          val evolvedSchema = new IcebergSchema(evolvedColumns)
          val operations = icebergTable.asInstanceOf[HasTableOperations].operations()
          val baseMetadata = operations.current()
          operations.commit(
            baseMetadata,
            TableMetadata
              .buildFrom(baseMetadata)
              .setCurrentSchema(evolvedSchema, evolvedSchema.highestFieldId())
              .build())

          val persistedSchema =
            SchemaParser.toJson(Spark3Util.loadIcebergTable(spark, probeTable).schema())
          assert(
            persistedSchema.contains("initial-default"),
            s"expected initial-default to persist into the committed schema, got: $persistedSchema")

          spark.sql(s"REFRESH TABLE $probeTable")
          val defaultedValues = spark
            .sql(s"SELECT c FROM $probeTable ORDER BY id")
            .collect()
            .toSeq
            .map(row => if (row.isNullAt(0)) "NULL" else row.getInt(0).toString)
          println(
            "DIAG columnDefault.readApplyProbe: read of defaulted column over pre-existing rows = " +
              s"[${defaultedValues.mkString(",")}] (diagnostic read result)")
        }
      }
    }
  }

  /** Removes the probe warehouse and everything the probe catalog wrote beneath it, deepest entry first. */
  private def deleteRecursively(directory: Path): Unit = {
    val paths = Files.walk(directory)
    try {
      paths
        .sorted(Comparator.reverseOrder[Path]())
        .iterator()
        .asScala
        .foreach(path => Files.delete(path))
    } finally {
      paths.close()
    }
  }
}
