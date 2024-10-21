package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

public class SparkCatalogTest extends SparkCatalog {
  @Override
  public SparkTable createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    // INSERT HMS CHECK LOGIC HERE.
    return super.createTable(ident, schema, transforms, properties);
  }
}
