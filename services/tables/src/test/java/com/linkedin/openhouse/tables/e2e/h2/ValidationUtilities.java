package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.dto.mapper.attribute.TimePartitionSpecConverter;
import com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.springframework.test.web.servlet.MvcResult;

/**
 * Home to all validation utility methods for each metadata field between raw {@link MvcResult} and
 * the metadata object.
 */
public final class ValidationUtilities {
  /** When major version bumps occur, this value will need to be modified. */
  public static final String CURRENT_MAJOR_VERSION_PREFIX = "/v1";

  private ValidationUtilities() {
    // util ctor noop
  }

  static void validateSchema(MvcResult result, String expectedSchemaJson)
      throws UnsupportedEncodingException {
    String returnSchemaJson = JsonPath.read(result.getResponse().getContentAsString(), "$.schema");

    // direct schema comparison will fail because OpenHouse drops name/namespace
    Assertions.assertTrue(
        getSchemaFromSchemaJson(expectedSchemaJson)
            .sameSchema(getSchemaFromSchemaJson(returnSchemaJson)));
  }

  /**
   * validation which verify all policies json are as per the Policies class in the return value.
   */
  static void validatePolicies(MvcResult result, Policies policies)
      throws UnsupportedEncodingException {
    int tableRetentionDays =
        JsonPath.read(result.getResponse().getContentAsString(), "$.policies.retention.count");

    Assertions.assertEquals(tableRetentionDays, policies.getRetention().getCount());
  }

  /**
   * validation which verify all policies json are as per the Policies class in the return value.
   */
  static void tableReplicationConfig(MvcResult result, Policies policies)
      throws UnsupportedEncodingException {
    if (policies.getReplication() != null) {
      HashMap<String, String> tableReplicationConfig =
          JsonPath.read(
              result.getResponse().getContentAsString(),
              "$.policies.replication.schedules[0].config");

      Assertions.assertEquals(
          policies.getReplication().getSchedules().get(0).getConfig(), tableReplicationConfig);
    }
  }

  static void validateUUID(MvcResult result, String uuid) throws UnsupportedEncodingException {
    String providedUuid = JsonPath.read(result.getResponse().getContentAsString(), "$.tableUUID");
    Assertions.assertEquals(providedUuid, uuid);
  }

  /**
   * Weak validation which only verify all writable properties in providedProperties exists in the
   * return value.
   *
   * <p>Weak validation is good enough and full map comparison is not applicable given the provided
   * properties usually don't include server generated properties (e.g. openhouse.tableId)
   */
  static void validateWritableTableProperties(MvcResult result, Map<String, String> originalProps)
      throws UnsupportedEncodingException {
    Map<String, String> updatedProps =
        JsonPath.read(result.getResponse().getContentAsString(), "$.tableProperties");
    validateWritableTableProperties(updatedProps, originalProps);
  }

  /**
   * Same method as above {@link #validateWritableTableProperties(MvcResult, Map)} with different
   * signature.
   */
  static void validateWritableTableProperties(
      Map<String, String> updatedProps, Map<String, String> originalProps) {
    for (Map.Entry<String, String> entry : originalProps.entrySet()) {
      if (!HouseTableSerdeUtils.IS_OH_PREFIXED.test(entry.getKey())) {
        Assertions.assertEquals(entry.getValue(), updatedProps.get(entry.getKey()));
      }
    }
  }

  static void validateLocation(MvcResult result, String rootPath)
      throws UnsupportedEncodingException {
    String databaseId = JsonPath.read(result.getResponse().getContentAsString(), "$.databaseId");
    String tableId = JsonPath.read(result.getResponse().getContentAsString(), "$.tableId");
    String tableUUID = JsonPath.read(result.getResponse().getContentAsString(), "$.tableUUID");
    String tableLocation =
        JsonPath.read(result.getResponse().getContentAsString(), "$.tableLocation");
    Path expectedPath = Paths.get("file:", rootPath, databaseId, tableId + "-" + tableUUID);
    Assertions.assertTrue(tableLocation.startsWith(expectedPath.toString()));
  }

  static void validateTimestamp(
      MvcResult result, long lastModTime, long creationTime, boolean trueUpdate)
      throws UnsupportedEncodingException {
    long updatedModTime =
        JsonPath.read(result.getResponse().getContentAsString(), "$.lastModifiedTime");
    long updatedCreationTime =
        JsonPath.read(result.getResponse().getContentAsString(), "$.creationTime");

    if (trueUpdate) {
      Assertions.assertTrue(updatedModTime > lastModTime);
    }
    Assertions.assertEquals(creationTime, updatedCreationTime);
  }

  static void validateTimePartition(MvcResult result, TimePartitionSpec timePartitionSpec)
      throws UnsupportedEncodingException {
    Assertions.assertEquals(
        new TimePartitionSpecConverter()
            .convertToEntityAttribute(
                JsonPath.read(result.getResponse().getContentAsString(), "$.timePartitioning")
                    .toString()),
        timePartitionSpec);
  }

  /**
   * openhouse.tableVersion and openhouse.tableLocation tends to be changed across success PUT
   * request. When comparing tableProperties within response, avoid comparing those by excluding the
   * comparison.
   */
  static boolean tblPropsResponseComparisonHelper(
      Map<String, String> originalProps, Map<String, String> updatedProps) {
    if (originalProps.size() != updatedProps.size()) {
      return false;
    }

    for (Map.Entry<String, String> entry : originalProps.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      // Skip the two keys to be excluded
      if ("openhouse.lastModifiedTime".equals(key)
          || "openhouse.tableLocation".equals(key)
          || "openhouse.tableVersion".equals(key)) {
        continue;
      }

      // Check if the other keys and values are equal
      if (!updatedProps.containsKey(key) || !updatedProps.get(key).equals(value)) {
        return false;
      }
    }

    return true;
  }

  static void validateMetadataInPutSnapshotsRequest(
      MvcResult result, IcebergSnapshotsRequestBody icebergSnapshotsRequestBody)
      throws UnsupportedEncodingException {
    CreateUpdateTableRequestBody expectedRequestBody =
        icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody();
    validateSchema(result, expectedRequestBody.getSchema());
    validateWritableTableProperties(result, expectedRequestBody.getTableProperties());
    validatePolicies(result, expectedRequestBody.getPolicies());
    ValidationUtilities.validateTimePartition(result, expectedRequestBody.getTimePartitioning());
  }

  static void validateSnapshots(
      Catalog catalog, MvcResult result, IcebergSnapshotsRequestBody icebergSnapshotsRequestBody)
      throws UnsupportedEncodingException {
    String tableLocation =
        JsonPath.read(result.getResponse().getContentAsString(), "$.tableLocation");
    List<Snapshot> putSnapshots =
        IcebergSnapshotsModelTestUtilities.obtainSnapshotsFromTableLoc(tableLocation);

    FileIO io =
        catalog
            .loadTable(
                TableIdentifier.of(
                    icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody().getDatabaseId(),
                    icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody().getTableId()))
            .io();
    List<Snapshot> expectedSnapshots =
        icebergSnapshotsRequestBody.getJsonSnapshots().stream()
            .map(s -> SnapshotParser.fromJson(s))
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedSnapshots, putSnapshots);
  }
}
