package com.linkedin.openhouse.tables.mock.api;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.validator.IcebergSnapshotsApiValidator;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class IcebergSnapshotsApiValidatorTest {
  private static final CreateUpdateTableRequestBody VALID_METADATA_BODY =
      CreateUpdateTableRequestBody.builder()
          .databaseId("d1")
          .tableId("t")
          .clusterId("c")
          .schema(HEALTH_SCHEMA_LITERAL)
          .policies(TABLE_POLICIES)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .columnName("timestamp")
                  .build())
          .tableProperties(ImmutableMap.of())
          .baseTableVersion("v0")
          .build();

  @Autowired private ApplicationContext appContext;

  @Test
  public void tmp() {
    try {
      Object bean = appContext.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  @Autowired private IcebergSnapshotsApiValidator icebergSnapshotsApiValidator;

  @Test
  public void testSuccessSnapshotsRequest() {
    assertDoesNotThrow(
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                VALID_METADATA_BODY.getDatabaseId(),
                VALID_METADATA_BODY.getTableId(),
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(
                        VALID_METADATA_BODY
                            .toBuilder()
                            .baseTableVersion(INITIAL_TABLE_VERSION)
                            .build())
                    .baseTableVersion(INITIAL_TABLE_VERSION)
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testEmptyMetadataRequest() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                VALID_METADATA_BODY.getDatabaseId(),
                VALID_METADATA_BODY.getTableId(),
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(null)
                    .baseTableVersion("v0")
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testSnapshotsUnmatchedId() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                "d1_d", /* database Id being different*/
                "t",
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(VALID_METADATA_BODY)
                    .baseTableVersion(INITIAL_TABLE_VERSION)
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                "d1",
                "t_t", /* table Id being different*/
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(VALID_METADATA_BODY)
                    .baseTableVersion(
                        "v2") // this triggers table id validation since table already exists
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testSnapshotsNullBaseVersion() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                VALID_METADATA_BODY.getDatabaseId(),
                VALID_METADATA_BODY.getTableId(),
                // Health Request body except base version being null
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(VALID_METADATA_BODY)
                    .baseTableVersion(null)
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testInconsistentBaseVersion() {
    /* VALID_METADATA_BODY is at v0 */
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                VALID_METADATA_BODY.getDatabaseId(),
                VALID_METADATA_BODY.getTableId(),
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(VALID_METADATA_BODY)
                    .baseTableVersion("v_random")
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testSnapshotsWithStagedCreate() {
    CreateUpdateTableRequestBody stagedCreateUpdateTableRequestBody =
        CreateUpdateTableRequestBody.builder()
            .databaseId("d1")
            .tableId("t")
            .clusterId("c")
            .schema(HEALTH_SCHEMA_LITERAL)
            .baseTableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(ImmutableMap.of())
            .stageCreate(true)
            .build();

    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                icebergSnapshotsApiValidator.validatePutSnapshots(
                    VALID_METADATA_BODY.getClusterId(),
                    VALID_METADATA_BODY.getDatabaseId(),
                    VALID_METADATA_BODY.getTableId(),
                    // Health Request body except base version being null
                    IcebergSnapshotsRequestBody.builder()
                        .createUpdateTableRequestBody(stagedCreateUpdateTableRequestBody)
                        .baseTableVersion(INITIAL_TABLE_VERSION)
                        .jsonSnapshots(Collections.emptyList())
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains("Creating staged table d1.t with putSnapshot is not supported"));
  }

  /**
   * Since the {@link com.linkedin.openhouse.tables.api.validator.TablesApiValidator} is reused in
   * the implementation, the test for validating {@link
   * com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody} doesn't need to
   * be comprehensive in terms of the metadata field.
   */
  @Test
  public void testSnapshotsSimpleInvalidMetadataBody() {
    // When required field(schema) is missing in metadata body
    CreateUpdateTableRequestBody missingSchema =
        CreateUpdateTableRequestBody.builder()
            .databaseId("d1")
            .tableId("t")
            .clusterId("c")
            .tableProperties(ImmutableMap.of())
            .build();

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                VALID_METADATA_BODY.getClusterId(),
                "d1",
                "t",
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(missingSchema)
                    .baseTableVersion(INITIAL_TABLE_VERSION)
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }

  @Test
  public void testPutSnapshotsClusterIdMismatch() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            icebergSnapshotsApiValidator.validatePutSnapshots(
                "local-cluster",
                VALID_METADATA_BODY.getDatabaseId(),
                VALID_METADATA_BODY.getTableId(),
                IcebergSnapshotsRequestBody.builder()
                    .createUpdateTableRequestBody(VALID_METADATA_BODY)
                    .baseTableVersion(INITIAL_TABLE_VERSION)
                    .jsonSnapshots(Collections.emptyList())
                    .build()));
  }
}
