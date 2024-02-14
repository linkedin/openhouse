package com.linkedin.openhouse.tables.settable;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.stereotype.Component;

@Component
public class SettableCatalogForTest extends OpenHouseInternalCatalog {
  private OpenHouseInternalTableOperations ops;

  public void setOperation(OpenHouseInternalTableOperations ops) {
    this.ops = ops;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return ops;
  }
}
