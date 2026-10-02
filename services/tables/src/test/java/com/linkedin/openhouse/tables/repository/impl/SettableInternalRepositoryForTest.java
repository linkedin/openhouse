package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;

public class SettableInternalRepositoryForTest extends OpenHouseInternalRepositoryImpl {

  public void setCatalog(OpenHouseInternalCatalog catalog) {
    this.catalog = catalog;
  }
}
