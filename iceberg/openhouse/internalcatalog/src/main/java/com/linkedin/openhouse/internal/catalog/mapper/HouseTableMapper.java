package com.linkedin.openhouse.internal.catalog.mapper;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.ENTITY_TYPE_TABLE;
import static com.linkedin.openhouse.internal.catalog.CatalogConstants.ENTITY_TYPE_VIEW;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.IS_OH_PREFIXED;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.OPENHOUSE_NAMESPACE;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.housetables.client.model.UserTable;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.springframework.beans.factory.annotation.Autowired;

@Mapper(componentModel = "spring")
public abstract class HouseTableMapper {

  @Autowired FileIOManager fileIOManager;

  @Mapping(target = "lastModifiedTime", ignore = true)
  @Mapping(
      target = "storageType",
      expression = "java(fileIOManager.getStorage(fileIO).getType().getValue())")
  public abstract HouseTable toHouseTable(Map<String, String> properties, FileIO fileIO);

  public HouseTable toHouseTable(TableMetadata tableMetadata, FileIO fileIO) {
    return toHouseTable(extractRawHTSFields(tableMetadata.properties()), fileIO);
  }

  public HouseTable toHouseTable(
      TableMetadata tableMetadata, FileIO fileIO, TableIdentifier tableIdentifier) {
    Map<String, String> properties = extractRawHTSFields(tableMetadata.properties());
    String clusterId = tableMetadata.properties().get(CatalogConstants.OPENHOUSE_CLUSTERID_KEY);
    properties.put("databaseId", tableIdentifier.namespace().toString());
    properties.put("tableId", tableIdentifier.name());
    properties.remove("tableUri");
    if (clusterId != null) {
      properties.put(
          "tableUri",
          TableUri.builder()
              .clusterId(clusterId)
              .databaseId(tableIdentifier.namespace().toString())
              .tableId(tableIdentifier.name())
              .build()
              .toString());
    }
    return toHouseTable(properties, fileIO);
  }

  @BeanMapping(ignoreByDefault = true)
  @Mapping(target = "databaseId", source = "userTable.databaseId")
  public abstract HouseTable toHouseTableWithDatabaseId(UserTable userTable);

  @BeanMapping(ignoreByDefault = true)
  @Mapping(target = "databaseId", source = "houseTable.databaseId")
  public abstract UserTable toUserTableWithDatabaseId(HouseTable houseTable);

  @Mappings({
    @Mapping(target = "tableLocation", source = "userTable.metadataLocation"),
    @Mapping(target = "entityType", source = "userTable.entityType")
  })
  public abstract HouseTable toHouseTable(UserTable userTable);

  /** Stamps TABLE here so no table-shaped caller can omit the required discriminator. */
  @Mappings({
    @Mapping(target = "metadataLocation", source = "houseTable.tableLocation"),
    @Mapping(target = "entityType", constant = ENTITY_TYPE_TABLE)
  })
  public abstract UserTable toUserTable(HouseTable houseTable);

  @Mappings({
    @Mapping(target = "metadataLocation", source = "houseTable.tableLocation"),
    @Mapping(target = "entityType", constant = ENTITY_TYPE_VIEW)
  })
  public abstract UserTable toUserView(HouseTable houseTable);

  private Map<String, String> extractRawHTSFields(Map<String, String> input) {
    Map<String, String> output = new HashMap<>();
    for (Map.Entry<String, String> entry : input.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (isHtsField(key)) {
        String newKey = stripOhNamespace(key);
        output.put(newKey, value);
      }
    }
    return output;
  }

  private static boolean isHtsField(String key) {
    return IS_OH_PREFIXED.test(key)
        && HouseTableSerdeUtils.HTS_FIELD_NAMES.contains(stripOhNamespace(key));
  }

  /** Private so MapStruct cannot adopt it as an implicit String mapping for every field value. */
  private static String stripOhNamespace(String key) {
    return IS_OH_PREFIXED.test(key) ? key.substring(OPENHOUSE_NAMESPACE.length()) : key;
  }
}
