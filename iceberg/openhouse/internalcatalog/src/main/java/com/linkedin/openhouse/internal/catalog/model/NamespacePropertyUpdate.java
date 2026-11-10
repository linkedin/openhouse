package com.linkedin.openhouse.internal.catalog.model;

import java.util.List;
import lombok.Value;

@Value
public class NamespacePropertyUpdate {
  List<String> updated;
  List<String> removed;
  List<String> missing;
}
