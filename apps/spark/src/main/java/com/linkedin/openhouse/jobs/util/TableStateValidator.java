package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.exception.TableValidationException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The Validation class ensures running basic sanity testing after a maintenance job has executed to
 * ensure table, database state is not compromised
 */
@Slf4j
public final class TableStateValidator {

  private TableStateValidator() {}

  public static void validateTableInDB(SparkSession spark, TableIdentifier ti) {
    List<Row> tables =
        spark
            .sql(
                String.format(
                    TablesValidationQueries.SHOW_TABLES, ti.namespace().toString(), ti.name()))
            .collectAsList();
    if (!tables.stream()
        .map(r -> String.format("%s", r.getString(r.fieldIndex("tableName"))))
        .collect(Collectors.toList())
        .contains(ti.name())) {
      throw new TableValidationException(
          String.format("Table %s not listed in database %s", ti, ti.namespace()));
    }
  }

  // perform post job validation
  public static void run(SparkSession spark, String fqtn) {
    String quotedFqtn = SparkJobUtil.getQuotedFqtn(fqtn);
    try {
      validateTableInDB(spark, TableIdentifier.parse(fqtn));
      TablesValidationQueries.get("metadata")
          .forEach(qs -> spark.sql(String.format(qs, quotedFqtn)));
      TablesValidationQueries.get("table").forEach(qs -> spark.sql(String.format(qs, quotedFqtn)));
    } catch (Exception e) {
      throw new TableValidationException(
          String.format("Table state validation failed with exception: %s", e));
    }
  }
}
