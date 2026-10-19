package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.TableTypeMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;

/** Utilities used in repository implementation. */
public final class InternalRepositoryUtils {

  protected static final String POLICIES_KEY = "policies";

  private static final Set<String> EXCLUDE_PROPERTIES_LIST =
      new HashSet<>(Collections.singletonList(POLICIES_KEY));

  private InternalRepositoryUtils() {
    // Utils' class constructor, noop
  }

  /**
   * @param updateProperties Iceberg {@link UpdateProperties} object capturing changes in table
   *     properties.
   * @param existingTableProps Existing table properties as a map.
   * @param providedTableProps New table properties provided by users. Note that this is the
   *     complete properties map.
   * @return True if alteration of properties occurred.
   */
  static boolean alterPropIfNeeded(
      UpdateProperties updateProperties,
      Map<String, String> existingTableProps,
      Map<String, String> providedTableProps) {
    boolean propertyUpdated = false;

    Set<String> unsetKeys = new HashSet<>(existingTableProps.keySet());

    // Due to the SerDe process, null-map is possible.
    if (providedTableProps != null) {
      for (Map.Entry<String, String> entry : providedTableProps.entrySet()) {
        unsetKeys.remove(entry.getKey());
        if (!existingTableProps.containsKey(entry.getKey())
            || !existingTableProps.get(entry.getKey()).equals(entry.getValue())) {
          updateProperties.set(entry.getKey(), entry.getValue());
          propertyUpdated = true;
        }
      }

      if (!unsetKeys.isEmpty()) {
        unsetKeys.forEach(updateProperties::remove);
        propertyUpdated = true;
      }
    }
    return propertyUpdated;
  }

  /**
   * Return only the user defined table properties by excluding preserved keys as well as policies.
   */
  static Map<String, String> getUserTblProps(
      Map<String, String> rawTableProps, PreservedKeyChecker checker, TableDto tableDto) {
    Map<String, String> result = new HashMap<>(rawTableProps);
    rawTableProps.forEach(
        (k, v) -> {
          if (checker.isKeyPreservedForTable(k, tableDto)) {
            result.remove(k);
          }
        });

    /**
     * removing keys like policies which are added exclusively into table properties and are not
     * part of HouseTable
     */
    for (String property : EXCLUDE_PROPERTIES_LIST) {
      result.remove(property);
    }
    return result;
  }

  /**
   * Converting {@link Table} to {@link TableDto} ONLY when returning back to response, since
   * jsonSnapshots won't be set to a meaningful value here.
   */
  @VisibleForTesting
  static TableDto convertToTableDto(
      Table table,
      FileIOManager fileIOManager,
      PartitionSpecMapper partitionSpecMapper,
      PoliciesSpecMapper policiesMapper,
      TableTypeMapper tableTypeMapper) {
    /* Contains everything needed to populate dto */
    final Map<String, String> megaProps = table.properties();
    Storage storage = fileIOManager.getStorage(table.io());
    TableDto tableDto =
        TableDto.builder()
            .tableId(megaProps.get(getCanonicalFieldName("tableId")))
            .databaseId(megaProps.get(getCanonicalFieldName("databaseId")))
            .clusterId(megaProps.get(getCanonicalFieldName("clusterId")))
            .tableUri(megaProps.get(getCanonicalFieldName("tableUri")))
            .tableUUID(megaProps.get(getCanonicalFieldName("tableUUID")))
            .tableLocation(
                URI.create(
                        StringUtils.prependIfMissing(
                            // remove after resolving
                            // https://github.com/linkedin/openhouse/issues/121
                            megaProps.get(getCanonicalFieldName("tableLocation")),
                            storage.getClient().getEndpoint()))
                    .normalize()
                    .toString())
            .tableVersion(megaProps.get(getCanonicalFieldName("tableVersion")))
            .tableCreator(megaProps.get(getCanonicalFieldName("tableCreator")))
            .schema(IcebergSchemaHelper.getSchemaJsonFromSchema(table.schema()))
            .lastModifiedTime(safeParseLong("lastModifiedTime", megaProps))
            .creationTime(safeParseLong("creationTime", megaProps))
            .timePartitioning(partitionSpecMapper.toTimePartitionSpec(table))
            .clustering(partitionSpecMapper.toClusteringSpec(table))
            .policies(policiesMapper.toPoliciesObject(megaProps.get("policies")))
            .tableType(tableTypeMapper.toTableType(table))
            .jsonSnapshots(null)
            .tableProperties(megaProps)
            .sortOrder(SortOrderParser.toJson(table.sortOrder()))
            .build();

    return tableDto;
  }

  /**
   * Safely parse the time related field into String.
   *
   * @return 0 indicate the value not available.
   */
  private static long safeParseLong(String keyName, Map<String, String> megaProps) {
    String canonicalFieldName = getCanonicalFieldName(keyName);
    return megaProps.containsKey(canonicalFieldName)
        ? Long.parseLong(megaProps.get(canonicalFieldName))
        : 0;
  }

  /**
   * Existence of
   * com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations#rootMetadataFileLocation(org.apache.iceberg.TableMetadata,
   * int) mandates this method for now given the asymmetrical format of persisted metadata location
   * in HTS and client-visible table location.
   */
  static String getSchemeLessPath(String rawPath) {
    return URI.create(rawPath).getPath();
  }

  /** Provides definition on what is reserved properties and extract them from tblproperties map */
  @VisibleForTesting
  public static Map<String, String> extractPreservedProps(
      Map<String, String> inputPropMaps,
      TableDto tableDto,
      PreservedKeyChecker preservedKeyChecker) {
    return inputPropMaps.entrySet().stream()
        .filter(e -> preservedKeyChecker.isKeyPreservedForTable(e.getKey(), tableDto))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
