package com.linkedin.openhouse.housetables.model;

import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class NamespaceMetadata {
  String namespaceUuid;
  String namespaceName;
  Map<String, String> properties;
}
