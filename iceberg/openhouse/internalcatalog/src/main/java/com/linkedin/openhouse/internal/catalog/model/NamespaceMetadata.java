package com.linkedin.openhouse.internal.catalog.model;

import java.util.Map;
import lombok.Value;

@Value
public class NamespaceMetadata {
  String namespaceUuid;
  String namespaceName;
  Map<String, String> properties;
}
