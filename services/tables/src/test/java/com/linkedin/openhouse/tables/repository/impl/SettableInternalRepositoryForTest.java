package com.linkedin.openhouse.tables.repository.impl;

import org.apache.iceberg.catalog.Catalog;

public class SettableInternalRepositoryForTest extends OpenHouseInternalRepositoryImpl {

  public void setCatalog(Catalog catalog) {
    this.catalog = catalog;
  }
}
