package com.linkedin.openhouse.housetables.model;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.MapKeyColumn;
import javax.persistence.Table;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "database_row")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DatabaseRow {
  @Id
  @Column(name = "database_uuid", length = 36, updatable = false)
  private String databaseUuid;

  @Column(name = "database_name", length = 128, nullable = false, updatable = false)
  private String databaseName;

  @Column(
      name = "database_name_key",
      length = 128,
      nullable = false,
      unique = true,
      updatable = false)
  private String databaseNameKey;

  @ElementCollection
  @CollectionTable(name = "database_property", joinColumns = @JoinColumn(name = "database_uuid"))
  @MapKeyColumn(name = "property_key", columnDefinition = "VARBINARY(2048)")
  @Convert(attributeName = "key", converter = DatabasePropertyKeyConverter.class)
  @Column(name = "property_value", nullable = false, columnDefinition = "MEDIUMTEXT")
  private Map<String, String> properties = new HashMap<>();

  @Version private long version;

  public DatabaseRow(String databaseName, Map<String, String> properties) {
    this.databaseUuid = UUID.randomUUID().toString();
    this.databaseName = databaseName;
    this.databaseNameKey = databaseName.toLowerCase(Locale.ROOT);
    this.properties.putAll(properties);
  }
}
