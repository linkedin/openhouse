package com.linkedin.openhouse.spark;

public class SparkCatalog extends org.apache.iceberg.spark.SparkCatalog {
  public boolean useNullableQuerySchema() {
    // Preserve DataFrame nullability when writing to OH tables
    return false;
  }
}
