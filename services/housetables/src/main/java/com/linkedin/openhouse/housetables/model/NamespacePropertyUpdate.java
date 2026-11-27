package com.linkedin.openhouse.housetables.model;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class NamespacePropertyUpdate {
  List<String> updated;
  List<String> removed;
  List<String> missing;
}
