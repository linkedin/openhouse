package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import java.util.List;
import java.util.Set;

public interface ColumnOrderGenerator {
  List<List<String>> generateOrder(Set<String> availableColumns);
}
