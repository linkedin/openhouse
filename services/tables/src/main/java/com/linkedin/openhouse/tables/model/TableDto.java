package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.attribute.ClusteringSpecConverter;
import com.linkedin.openhouse.tables.dto.mapper.attribute.PoliciesSpecConverter;
import com.linkedin.openhouse.tables.dto.mapper.attribute.TimePartitionSpecConverter;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.Convert;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for persisting Table Object in OpenHouseCatalog. */
@Entity
@IdClass(TableDtoPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableDto {

  @Id private String tableId;

  @Id private String databaseId;

  private String clusterId;

  private String tableUri;

  private String tableUUID;

  private String tableLocation;

  private String tableVersion;

  private String tableCreator;

  private String schema;

  private long lastModifiedTime;

  private long creationTime;

  private TableType tableType;

  private String sortOrder;

  @Convert(converter = TimePartitionSpecConverter.class)
  private TimePartitionSpec timePartitioning;

  @Convert(converter = ClusteringSpecConverter.class)
  @ElementCollection
  private List<ClusteringColumn> clustering;

  @ElementCollection private List<String> jsonSnapshots;

  @ElementCollection private Map<String, String> snapshotRefs;

  @Convert(converter = PoliciesSpecConverter.class)
  private Policies policies;

  @ElementCollection private Map<String, String> tableProperties;

  @ElementCollection private List<String> newIntermediateSchemas;

  private boolean stageCreate;

  /**
   * Bundling eligible string type field into a map as {@link org.mapstruct.Mapper} doesn't provide
   * easy interface to achieve so.
   */
  public Map<String, String> convertToMap() {
    Map<String, String> map = new HashMap<>();
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (isTableDtoPrimitive(field)
          && !(field.getName().equals("schema") || field.getName().equals("jsonSnapshots"))) {
        try {
          String name = field.getName();
          Object fieldVal = field.get(this);
          String value = fieldVal == null ? null : fieldVal.toString();
          map.put(name, value);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              "Cannot converting TableDTO object into a Map due to access issue for the field:"
                  + field.getName(),
              e);
        }
      }
    }
    return map;
  }

  private boolean isTableDtoPrimitive(Field field) {
    return field.getType() == String.class || field.getType() == long.class;
  }
}
