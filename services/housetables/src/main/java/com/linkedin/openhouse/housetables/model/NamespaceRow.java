package com.linkedin.openhouse.housetables.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "catalog_namespaces")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NamespaceRow {
  @Id
  @Column(name = "namespace_uuid", length = 36, updatable = false)
  private String namespaceUuid;

  @Column(name = "namespace_name", length = 128, nullable = false, unique = true, updatable = false)
  private String namespaceName;

  @Convert(converter = NamespacePropertiesConverter.class)
  @Column(name = "properties", nullable = false, columnDefinition = "LONGTEXT")
  private Map<String, String> properties = new HashMap<>();

  @Version private long version;

  public NamespaceRow(String namespaceName, Map<String, String> properties) {
    this.namespaceUuid = UUID.randomUUID().toString();
    this.namespaceName = namespaceName;
    this.properties.putAll(properties);
  }

  public void replaceProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }
}
