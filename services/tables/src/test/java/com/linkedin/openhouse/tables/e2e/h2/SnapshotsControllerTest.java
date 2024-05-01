package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.e2e.h2.RequestAndValidateHelper.putSnapshotsAndValidateResponse;
import static com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tablestest.annotation.CustomParameterResolver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/** A dedicated test classes of e2e controller tests for Snapshot API. */
@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(CustomParameterResolver.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@WithMockUser(username = "testUser")
public class SnapshotsControllerTest {
  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  @Autowired MockMvc mvc;

  @Autowired FsStorageProvider fsStorageProvider;

  @Autowired FileIO fileIo;

  /** For now starting with a naive object feeder. */
  private static Stream<GetTableResponseBody> responseBodyFeeder() {
    return Stream.of(GET_TABLE_RESPONSE_BODY);
  }

  private static Stream<Arguments> responseBodyFeederCreateReservedProps() {
    return Stream.of(Arguments.of(GET_TABLE_RESPONSE_BODY_RESERVED_PROP));
  }

  private static Stream<Arguments> responseBodyFeederUpdateReservedProps() {
    return Stream.of(Arguments.of(GET_TABLE_RESPONSE_BODY, GET_TABLE_RESPONSE_BODY_RESERVED_PROP));
  }

  @ParameterizedTest
  @MethodSource("responseBodyFeeder")
  public void testPutSnapshotsAppend(GetTableResponseBody getTableResponseBody) throws Exception {
    String dataFilePath = fsStorageProvider.rootPath() + "/data.orc";

    MvcResult createResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBody, mvc, fsStorageProvider);
    GetTableResponseBody getResponseBody = buildGetTableResponseBody(createResult);
    IcebergSnapshotsRequestBody icebergSnapshotRequestBody =
        preparePutSnapshotsWithAppendRequest(
            createResult,
            getResponseBody,
            getResponseBody.getTableLocation(),
            catalog,
            Collections.singletonList(
                createDummyDataFile(dataFilePath, getPartitionSpec(getTableResponseBody))));
    putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);
  }

  @ParameterizedTest
  @MethodSource("responseBodyFeeder")
  public void testPutSnapshotsAppendWithStagedTable(GetTableResponseBody getTableResponseBody)
      throws Exception {
    MvcResult stagedResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBody, mvc, fsStorageProvider, true);

    String beforeUUID =
        JsonPath.read(stagedResult.getResponse().getContentAsString(), "$.tableUUID");
    List<String> jsonSnapshots =
        ImmutableList.of(
            getValidSnapshot(
                getTableResponseBody
                    .toBuilder()
                    .tableVersion(INITIAL_TABLE_VERSION)
                    .tableProperties(tablePropsHelperForResponseBody(getTableResponseBody))
                    .tableUUID(beforeUUID)
                    .build()));
    Map<String, String> snapshotRefs =
        obtainSnapshotRefsFromSnapshot(jsonSnapshots.get(jsonSnapshots.size() - 1));

    IcebergSnapshotsRequestBody icebergSnapshotRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(INITIAL_TABLE_VERSION)
            .createUpdateTableRequestBody(
                buildCreateUpdateTableRequestBody(getTableResponseBody)
                    .toBuilder()
                    .baseTableVersion(INITIAL_TABLE_VERSION)
                    .tableProperties(tablePropsHelperForResponseBody(getTableResponseBody))
                    .build())
            .jsonSnapshots(jsonSnapshots)
            .snapshotRefs(snapshotRefs)
            .build();

    MvcResult commitResult =
        putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, true);
    ValidationUtilities.validateUUID(commitResult, beforeUUID);
  }

  @ParameterizedTest
  @MethodSource("responseBodyFeeder")
  public void testPutSnapshotsDelete(GetTableResponseBody getTableResponseBody) throws Exception {
    String dataFilePath1 = fsStorageProvider.rootPath() + "/data1.orc";
    String dataFilePath2 = fsStorageProvider.rootPath() + "/data2.orc";
    MvcResult createResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBody, mvc, fsStorageProvider);
    GetTableResponseBody getResponseBody = buildGetTableResponseBody(createResult);

    // append once
    IcebergSnapshotsRequestBody icebergSnapshotRequestBody =
        preparePutSnapshotsWithAppendRequest(
            createResult,
            getResponseBody,
            getResponseBody.getTableLocation(),
            catalog,
            Collections.singletonList(
                createDummyDataFile(dataFilePath1, getPartitionSpec(getTableResponseBody))));
    MvcResult putResult =
        putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);

    // append twice
    icebergSnapshotRequestBody =
        preparePutSnapshotsWithAppendRequest(
            putResult,
            getResponseBody,
            RequestAndValidateHelper.getCurrentTableLocation(
                mvc, getResponseBody.getDatabaseId(), getResponseBody.getTableId()),
            catalog,
            Collections.singletonList(
                createDummyDataFile(dataFilePath2, getPartitionSpec(getTableResponseBody))));
    putResult = putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);

    // delete first snapshot
    icebergSnapshotRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(
                RequestAndValidateHelper.getCurrentTableLocation(
                    mvc, getResponseBody.getDatabaseId(), getResponseBody.getTableId()))
            .createUpdateTableRequestBody(buildCreateUpdateTableRequestBody(putResult))
            .jsonSnapshots(icebergSnapshotRequestBody.getJsonSnapshots().subList(1, 2))
            .snapshotRefs(
                obtainSnapshotRefsFromSnapshot(
                    icebergSnapshotRequestBody.getJsonSnapshots().get(1)))
            .build();
    putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);
  }

  @ParameterizedTest
  @MethodSource("responseBodyFeeder")
  public void testPutSnapshotsAppendMultiple(GetTableResponseBody getTableResponseBody)
      throws Exception {
    String dataFilePath1 = fsStorageProvider.rootPath() + "/data1.orc";
    String dataFilePath2 = fsStorageProvider.rootPath() + "/data2.orc";
    MvcResult createResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBody, mvc, fsStorageProvider);
    GetTableResponseBody getResponseBody = buildGetTableResponseBody(createResult);

    // get old and new snapshots
    List<List<DataFile>> dataFilesRequests = new ArrayList<>();
    dataFilesRequests.add(
        Collections.singletonList(
            createDummyDataFile(dataFilePath1, getPartitionSpec(getTableResponseBody))));
    dataFilesRequests.add(
        Collections.singletonList(
            createDummyDataFile(dataFilePath2, getPartitionSpec(getTableResponseBody))));
    List<String> snapshots =
        getSnapshotsWithMultipleAppendRequests(getTableResponseBody, catalog, dataFilesRequests)
            .stream()
            .map(SnapshotParser::toJson)
            .collect(Collectors.toList());

    // put 2 new snapshots
    IcebergSnapshotsRequestBody icebergSnapshotRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(
                RequestAndValidateHelper.getCurrentTableLocation(
                    mvc, getResponseBody.getDatabaseId(), getResponseBody.getTableId()))
            .createUpdateTableRequestBody(buildCreateUpdateTableRequestBody(createResult))
            .jsonSnapshots(snapshots)
            .snapshotRefs(obtainSnapshotRefsFromSnapshot(snapshots.get(snapshots.size() - 1)))
            .build();
    putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);
  }

  @ParameterizedTest
  @MethodSource("responseBodyFeeder")
  public void testPutSnapshotsReplicaTableType(GetTableResponseBody getTableResponseBody)
      throws Exception {
    String dataFilePath1 = fsStorageProvider.rootPath() + "/data1.orc";
    String dataFilePath2 = fsStorageProvider.rootPath() + "/data2.orc";
    Map<String, String> propsMap = new HashMap<>();
    propsMap.put("openhouse.tableUUID", "cee3c6a3-a824-443a-832a-d4a1271e1e3e");
    propsMap.put("openhouse.databaseId", getTableResponseBody.getDatabaseId());
    propsMap.put("openhouse.tableId", getTableResponseBody.getTableId());

    MvcResult createResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBody
                .toBuilder()
                .tableType(TableType.REPLICA_TABLE)
                .tableProperties(propsMap)
                .build(),
            mvc,
            fsStorageProvider);
    GetTableResponseBody getResponseBody = buildGetTableResponseBody(createResult);

    // get old and new snapshots
    List<List<DataFile>> dataFilesRequests = new ArrayList<>();
    dataFilesRequests.add(
        Collections.singletonList(
            createDummyDataFile(dataFilePath1, getPartitionSpec(getTableResponseBody))));
    dataFilesRequests.add(
        Collections.singletonList(
            createDummyDataFile(dataFilePath2, getPartitionSpec(getTableResponseBody))));
    List<String> snapshots =
        getSnapshotsWithMultipleAppendRequests(getTableResponseBody, catalog, dataFilesRequests)
            .stream()
            .map(SnapshotParser::toJson)
            .collect(Collectors.toList());

    CreateUpdateTableRequestBody requestBody =
        buildCreateUpdateTableRequestBody(createResult).toBuilder().build();
    propsMap = requestBody.getTableProperties();
    propsMap.put("openhouse.tableType", "REPLICA_TABLE");

    // put 2 new snapshots
    IcebergSnapshotsRequestBody icebergSnapshotRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(
                RequestAndValidateHelper.getCurrentTableLocation(
                    mvc, getResponseBody.getDatabaseId(), getResponseBody.getTableId()))
            .createUpdateTableRequestBody(
                requestBody
                    .toBuilder()
                    .tableType(TableType.REPLICA_TABLE)
                    .tableProperties(propsMap)
                    .build())
            .jsonSnapshots(snapshots)
            .snapshotRefs(obtainSnapshotRefsFromSnapshot(snapshots.get(snapshots.size() - 1)))
            .build();
    putSnapshotsAndValidateResponse(catalog, mvc, icebergSnapshotRequestBody, false);
  }

  @AfterEach
  private void deleteTable(GetTableResponseBody getTableResponseBody) {
    Optional<TableDto> tableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder()
                .databaseId(getTableResponseBody.getDatabaseId())
                .tableId(getTableResponseBody.getTableId())
                .build());
    tableDto.ifPresent(dto -> openHouseInternalRepository.delete(dto));
  }

  @SneakyThrows
  private String getValidSnapshot(GetTableResponseBody getTableResponseBody) {
    openHouseInternalRepository.save(buildTableDto(getTableResponseBody));
    String dataPath = fsStorageProvider.rootPath() + "/data.orc";
    DataFile dataFile = createDummyDataFile(dataPath, getPartitionSpec(getTableResponseBody));
    TableIdentifier tableIdentifier =
        TableIdentifier.of(getTableResponseBody.getDatabaseId(), getTableResponseBody.getTableId());
    Table table = catalog.loadTable(tableIdentifier);
    Snapshot newSnapshot = table.newAppend().appendFile(dataFile).apply();
    catalog.dropTable(tableIdentifier, false);
    return SnapshotParser.toJson(newSnapshot);
  }

  private List<Snapshot> getSnapshotsWithMultipleAppendRequests(
      GetTableResponseBody getTableResponseBody,
      Catalog catalog,
      List<List<DataFile>> dataFilesRequests) {

    List<Snapshot> snapshots = new ArrayList<>();
    TableIdentifier tableIdentifier =
        TableIdentifier.of(getTableResponseBody.getDatabaseId(), getTableResponseBody.getTableId());
    Table table = catalog.loadTable(tableIdentifier);
    snapshots.addAll(Lists.newArrayList(table.snapshots()));
    Transaction txn = table.newTransaction();
    for (List<DataFile> dataFiles : dataFilesRequests) {
      AppendFiles appendFiles = txn.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();
      Snapshot snapshot = txn.table().currentSnapshot();
      snapshots.add(snapshot);
    }
    return snapshots;
  }

  /**
   * For mock responseBody, ensure they are equipped with correct properties that are critical for
   * casing contract.
   */
  private Map<String, String> tablePropsHelperForResponseBody(GetTableResponseBody responseBody) {
    Map<String, String> originalProps = responseBody.getTableProperties();
    originalProps.put("openhouse.databaseId", responseBody.getDatabaseId());
    originalProps.put("openhouse.tableId", responseBody.getTableId());
    return originalProps;
  }
}
