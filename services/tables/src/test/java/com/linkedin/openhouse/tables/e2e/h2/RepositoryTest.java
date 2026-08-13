package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.AopTestUtils;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class RepositoryTest {

  @Autowired HouseTableRepository houseTablesRepository;

  @SpyBean @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired StorageManager storageManager;

  @Autowired Catalog catalog;

  @Autowired SchemaValidator validator;

  @SpyBean @Autowired PreservedKeyChecker preservedKeyChecker;

  @Test
  void extractReservedProps() {
    // create an input map with some key-value pairs
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("openhouse.key1", "value1");
    inputMap.put("otherKey1", "value2");
    inputMap.put("openhouse.key2", "value3");
    TableDto mockTableDto = Mockito.mock(TableDto.class);

    // call the method to extract the reserved props
    // Casting towards a specific implementation here.
    Map<String, String> result =
        InternalRepositoryUtils.extractPreservedProps(inputMap, mockTableDto, preservedKeyChecker);

    // assert that the result map has the expected size and contents
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("value1", result.get("openhouse.key1"));
    Assertions.assertEquals("value3", result.get("openhouse.key2"));

    // Test empty map make sure no exception
    inputMap.clear();
    result =
        InternalRepositoryUtils.extractPreservedProps(inputMap, mockTableDto, preservedKeyChecker);
    Assertions.assertEquals(0, result.size());
  }

  @Test
  void testPoliciesKeyIsPreserved() {
    // Verify that 'policies' is recognized as a preserved key
    Assertions.assertTrue(preservedKeyChecker.isKeyPreserved("policies"));
    Assertions.assertFalse(preservedKeyChecker.isKeyPreserved("userKey"));

    // Verify that extractPreservedProps includes 'policies'
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("openhouse.key1", "value1");
    inputMap.put("policies", "{\"retention\":{\"count\":3,\"granularity\":\"HOUR\"}}");
    inputMap.put("userKey", "userValue");
    TableDto mockTableDto = Mockito.mock(TableDto.class);

    Map<String, String> result =
        InternalRepositoryUtils.extractPreservedProps(inputMap, mockTableDto, preservedKeyChecker);

    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.containsKey("openhouse.key1"));
    Assertions.assertTrue(result.containsKey("policies"));
    Assertions.assertFalse(result.containsKey("userKey"));
  }

  @Test
  public void testUpdateTableWithModifiedPoliciesInTablePropsBlocked() {
    // Create a table with policies
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblPoliciesPreserved")
            .tableVersion(INITIAL_TABLE_VERSION)
            .policies(TABLE_POLICIES)
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);

    // Attempt to update the table with modified 'policies' in tableProperties
    Map<String, String> updatedTableProps = new HashMap<>(createdDto.getTableProperties());
    updatedTableProps.put("policies", "{}");
    TableDto updateDto =
        createdDto
            .toBuilder()
            .tableId("tblPoliciesPreserved")
            .tableVersion(createdDto.getTableLocation())
            .tableProperties(updatedTableProps)
            .build();

    UnsupportedClientOperationException e =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(updateDto));
    Assertions.assertTrue(e.getMessage().contains("policies"));

    // Cleanup
    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .tableId("tblPoliciesPreserved")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  public void testReplaceMergesExistingPolicies() {
    // Create a table with a retention policy and RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplaceMergesPolicies")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(TABLE_POLICIES_COMPLEX)
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertNotNull(createdDto.getPolicies());
    Assertions.assertNotNull(createdDto.getPolicies().getRetention());

    // CREATE OR REPLACE (RTAS) the table WITHOUT specifying policies.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(null)
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    // Policies must be carried forward (merged), not silently wiped.
    Assertions.assertNotNull(replacedDto.getPolicies(), "RTAS wiped the policies plane");
    Assertions.assertNotNull(
        replacedDto.getPolicies().getRetention(), "RTAS dropped the retention policy");
    Assertions.assertEquals(
        createdDto.getPolicies().getRetention().getCount(),
        replacedDto.getPolicies().getRetention().getCount(),
        "RTAS changed the retention policy");

    // Cleanup
    TableDtoPrimaryKey replacePk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplaceMergesPolicies")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(replacePk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(replacePk));
  }

  @Test
  public void testReplaceAppliesRequestedPolicies() {
    // Create a table with a retention policy (count=3) and RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplaceAppliesPolicies")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(3)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertEquals(3, createdDto.getPolicies().getRetention().getCount());

    // RTAS that PROVIDES a new retention policy -> the request's policy must be applied.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(8)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    Assertions.assertEquals(
        8,
        replacedDto.getPolicies().getRetention().getCount(),
        "RTAS should apply the retention policy provided on the request");

    // Cleanup
    TableDtoPrimaryKey appliesPk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplaceAppliesPolicies")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(appliesPk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(appliesPk));
  }

  @Test
  public void testReplaceWithPartialPoliciesPreservesSharing() {
    // Create a table with sharing enabled AND a retention policy, RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplacePreservesSharing")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(
                Policies.builder()
                    .sharingEnabled(true)
                    .retention(
                        Retention.builder()
                            .count(3)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertTrue(createdDto.getPolicies().isSharingEnabled());

    // RTAS with a PARTIAL policies payload (retention only, sharing omitted). sharingEnabled is a
    // primitive boolean, so an omitted value is indistinguishable from false; the merge must carry
    // the existing table's sharing forward rather than silently disable it.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(8)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    Assertions.assertTrue(
        replacedDto.getPolicies().isSharingEnabled(),
        "RTAS with a partial policies payload disabled sharing (expected it to be preserved)");
    Assertions.assertEquals(
        8,
        replacedDto.getPolicies().getRetention().getCount(),
        "RTAS should apply the retention policy provided on the request");

    // Cleanup
    TableDtoPrimaryKey pk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplacePreservesSharing")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(pk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(pk));
  }

  @Test
  public void testReplaceWithPartialPoliciesPreservesOmittedPlanes() {
    // Create a table with BOTH a retention and a history policy, RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplacePreservesOmittedPlanes")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(3)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .history(
                        History.builder()
                            .maxAge(24)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertEquals(24, createdDto.getPolicies().getHistory().getMaxAge());

    // RTAS that overrides ONLY retention. The omitted history plane must be carried forward.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(8)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    Assertions.assertEquals(
        8,
        replacedDto.getPolicies().getRetention().getCount(),
        "RTAS should apply the retention policy provided on the request");
    Assertions.assertNotNull(
        replacedDto.getPolicies().getHistory(),
        "RTAS dropped the omitted history plane (expected it to be carried forward)");
    Assertions.assertEquals(
        24,
        replacedDto.getPolicies().getHistory().getMaxAge(),
        "RTAS changed the carried-forward history policy");

    // Cleanup
    TableDtoPrimaryKey pk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplacePreservesOmittedPlanes")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(pk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(pk));
  }

  @Test
  public void testReplaceOverwritesColumnTags() {
    // Create a table whose only policy is a column tag on col1, RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplaceOverwritesColumnTags")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(
                Policies.builder()
                    .columnTags(
                        Collections.singletonMap(
                            "col1",
                            PolicyTag.builder()
                                .tags(Collections.singleton(PolicyTag.Tag.PII))
                                .build()))
                    .build())
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertTrue(createdDto.getPolicies().getColumnTags().containsKey("col1"));

    // RTAS that provides a NON-EMPTY column-tags map. Column tags use overwrite semantics: the
    // request's map replaces the existing map wholesale, so col1's tag is dropped and only col2's
    // is present.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(
                Policies.builder()
                    .columnTags(
                        Collections.singletonMap(
                            "col2",
                            PolicyTag.builder()
                                .tags(Collections.singleton(PolicyTag.Tag.HC))
                                .build()))
                    .build())
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    Assertions.assertTrue(
        replacedDto.getPolicies().getColumnTags().containsKey("col2"),
        "RTAS should apply the column tags provided on the request");
    Assertions.assertFalse(
        replacedDto.getPolicies().getColumnTags().containsKey("col1"),
        "column tags use overwrite semantics: the request's map should replace the existing map "
            + "wholesale, dropping col1");

    // Cleanup
    TableDtoPrimaryKey pk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplaceOverwritesColumnTags")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(pk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(pk));
  }

  @Test
  public void testReplaceWithPartialPoliciesPreservesColumnTags() {
    // Create a table whose only policy is a column tag on col1, RTAS enabled.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("tblReplacePreservesColumnTags")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(props)
            .policies(
                Policies.builder()
                    .columnTags(
                        Collections.singletonMap(
                            "col1",
                            PolicyTag.builder()
                                .tags(Collections.singleton(PolicyTag.Tag.PII))
                                .build()))
                    .build())
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);
    Assertions.assertTrue(createdDto.getPolicies().getColumnTags().containsKey("col1"));

    // RTAS with a non-empty policies payload that provides retention but omits column tags. Because
    // column tags treat an absent or empty map the same as "not provided," the existing tags must
    // be carried forward rather than wiped.
    TableDto replaceDto =
        createdDto
            .toBuilder()
            .tableVersion(createdDto.getTableLocation())
            .policies(
                Policies.builder()
                    .retention(
                        Retention.builder()
                            .count(8)
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .build())
            .replaceCommit(true)
            .build();
    TableDto replacedDto = openHouseInternalRepository.save(replaceDto);

    Assertions.assertEquals(
        8,
        replacedDto.getPolicies().getRetention().getCount(),
        "RTAS should apply the retention policy provided on the request");
    Assertions.assertTrue(
        replacedDto.getPolicies().getColumnTags().containsKey("col1"),
        "RTAS with a partial policies payload dropped the existing column tags (expected them to be "
            + "carried forward)");

    // Cleanup
    TableDtoPrimaryKey pk =
        TableDtoPrimaryKey.builder()
            .tableId("tblReplacePreservesColumnTags")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(pk);
    Assertions.assertFalse(openHouseInternalRepository.existsById(pk));
  }

  @Test
  public void testOpenHouseRepository() {
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();

    openHouseInternalRepository.save(creationDTO);

    TableDtoPrimaryKey key = getPrimaryKey(creationDTO);

    TableDto table =
        openHouseInternalRepository.findById(key).orElseThrow(NoSuchElementException::new);
    verifyTable(table);

    HouseTable houseTable =
        houseTablesRepository
            .findById(getHouseTablePrimaryKey(key))
            .orElseThrow(NoSuchElementException::new);
    verifyTable(houseTable);

    Assertions.assertTrue(openHouseInternalRepository.existsById(key));

    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testConcurrentRepoOps() {
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDtoPrimaryKey key = getPrimaryKey(creationDTO);
    openHouseInternalRepository.save(creationDTO);

    // Simulating a scenario of table-already-existed exception and verified exception it throws.
    TableDto existedDto = creationDTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    OpenHouseInternalRepository spyRepo = Mockito.spy(openHouseInternalRepository);
    Mockito.doReturn(false).when(spyRepo).existsById(key);

    Assertions.assertThrows(
        org.apache.iceberg.exceptions.AlreadyExistsException.class, () -> spyRepo.save(existedDto));

    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  /**
   * Testing edge cases for tblprops. For normal behavior see {@link
   * TablesControllerTest#testUpdateProperties()}
   */
  @Test
  public void testTblPropsCornerCases() {
    Map<String, String> userProps = new HashMap<>();
    userProps.put("tableId", "foo"); /* make sure such key shouldn't confuse table service*/
    userProps.put(
        TableProperties.DEFAULT_FILE_FORMAT, "avro"); /* make sure such key will be preserved */
    userProps.put(TableProperties.FORMAT_VERSION, "1"); /* make sure such key will be overwritten */
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(userProps)
            .build();

    TableDto returnedDto = openHouseInternalRepository.save(createDto);
    Assertions.assertNotNull(returnedDto.getTableProperties());
    Assertions.assertFalse(returnedDto.getTableProperties().isEmpty());
    Assertions.assertEquals(returnedDto.getTableProperties().get("tableId"), "foo");
    Assertions.assertEquals(
        returnedDto.getTableProperties().get("openhouse.tableId"), TABLE_DTO.getTableId());
    Assertions.assertEquals(
        returnedDto.getTableProperties().get(TableProperties.DEFAULT_FILE_FORMAT).toLowerCase(),
        "avro");
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    Assertions.assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
    Assertions.assertNull(returnedDto.getTableProperties().get(TableProperties.FORMAT_VERSION));
    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testOpenHouseInvalidClusteringEvolution() {

    TableDto tableDto =
        openHouseInternalRepository.save(
            TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());

    TableDto changeClusteringColDto =
        tableDto
            .toBuilder()
            .clustering(Arrays.asList(ClusteringColumn.builder().columnName("name").build()))
            .tableVersion(tableDto.getTableLocation())
            .build();

    UnsupportedClientOperationException unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(changeClusteringColDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto removeClusteringDto =
        tableDto.toBuilder().tableVersion(tableDto.getTableLocation()).clustering(null).build();

    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(removeClusteringDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto addClusteringDto =
        tableDto
            .toBuilder()
            .tableVersion(tableDto.getTableLocation())
            .clustering(
                Streams.concat(
                        Arrays.asList(ClusteringColumn.builder().columnName("count").build())
                            .stream(),
                        TABLE_DTO.getClustering().stream())
                    .collect(Collectors.toList()))
            .build();
    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(addClusteringDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testOpenHouseInvalidTimePartitioningEvolution() {
    TableDto createTableDto = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDto tableDto = openHouseInternalRepository.save(createTableDto);

    TableDto removeTimePartitionCol =
        tableDto
            .toBuilder()
            .timePartitioning(null)
            .tableVersion(tableDto.getTableLocation())
            .build();

    UnsupportedClientOperationException unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(removeTimePartitionCol));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto differentGranularityTimePartitionCol =
        tableDto
            .toBuilder()
            .timePartitioning(
                TimePartitionSpec.builder()
                    .columnName("timestampCol")
                    .granularity(TimePartitionSpec.Granularity.DAY)
                    .build())
            .tableVersion(tableDto.getTableLocation())
            .build();

    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(differentGranularityTimePartitionCol));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testExistsByIdThatDoesNotExist() {

    Assertions.assertFalse(
        openHouseInternalRepository.existsById(
            TableDtoPrimaryKey.builder().databaseId("not_found").tableId("not_found").build()));

    NullPointerException nullPointerException =
        Assertions.assertThrows(
            NullPointerException.class,
            () -> openHouseInternalRepository.existsById(TableDtoPrimaryKey.builder().build()));
    Assertions.assertEquals(
        "Cannot create a namespace with a null level", nullPointerException.getMessage());
  }

  @Test
  public void testFindAllIds() {
    openHouseInternalRepository.save(
        TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());
    openHouseInternalRepository.save(
        TABLE_DTO_DIFF_DB.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());
    Assertions.assertEquals(2, openHouseInternalRepository.findAllIds().size());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    TableDtoPrimaryKey keyDiffDb = getPrimaryKey(TABLE_DTO_DIFF_DB);
    openHouseInternalRepository.deleteById(key);
    openHouseInternalRepository.deleteById(keyDiffDb);
  }

  @Test
  public void testCreateTableWithReservedProps() {
    /* The behavior is provided openhouse. properties are ignored */
    final String tblName = "offensiveMap";

    Map<String, String> offensiveMap = new HashMap<>();
    offensiveMap.put("openhouse.tableId", "random");
    offensiveMap.put("openhouse.tableVersion", "random");
    offensiveMap.put("openhouse.tableLocation", "random");
    offensiveMap.put("openhouse.keepReadOnlyProp", "true");
    TableDto offensiveDto =
        TABLE_DTO
            .toBuilder()
            .tableId("offensiveMap")
            .tableProperties(offensiveMap)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();
    PreservedKeyChecker spyPreservedKeyChecker = Mockito.spy(preservedKeyChecker);
    // Test extending the preservedKeyChecker enables allowlisting of properties during table
    // creation
    Mockito.doReturn(true)
        .when(spyPreservedKeyChecker)
        .allowKeyInCreation(Mockito.eq("openhouse.keepReadOnlyProp"), Mockito.any());
    // Demonstrated the offensive setting doesn't matter.
    TableDto createdDTO = openHouseInternalRepository.save(offensiveDto);
    Assertions.assertEquals(createdDTO.getTableId(), tblName);
    Map<String, String> createdTableProps = createdDTO.getTableProperties();
    // Should not be overridden by the user provided value given that these should be filtered on
    // creation
    Assertions.assertNotEquals(createdTableProps.get("openhouse.tableVersions"), "random");
    Assertions.assertNotEquals(createdTableProps.get("openhouse.tableLocation"), "random");
    Assertions.assertEquals(createdTableProps.get("openhouse.keepReadOnlyProp"), "true");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder().tableId(tblName).databaseId(TABLE_DTO.getDatabaseId()).build();

    Map<String, String> updatedTableProps = new HashMap<>();
    updatedTableProps.putAll(createdTableProps);
    updatedTableProps.put("openhouse.keepReadOnlyProp", "false");
    TableDto updatedOffensiveDto =
        TABLE_DTO
            .toBuilder()
            .tableId("offensiveMap")
            .tableProperties(updatedTableProps)
            .tableVersion(createdDTO.getTableLocation())
            .build();
    // Should fail due to updating preserved keys
    UnsupportedClientOperationException e =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(updatedOffensiveDto));
    Assertions.assertTrue(
        e.getMessage()
            .startsWith(
                "Bad tblproperties provided: Can't add, alter or drop table properties due to the restriction: "
                    + "[table properties starting with `openhouse.` and the `policies` key cannot be modified], diff in existing "
                    + "& provided table properties: {"));

    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  public void testOpenHouseRepositoryForStagedTable() {
    TableDto returnedTableDto =
        openHouseInternalRepository.save(TableModelConstants.STAGED_TABLE_DTO);
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getTableId(), returnedTableDto.getTableId());
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getDatabaseId(), returnedTableDto.getDatabaseId());
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getClusterId(), returnedTableDto.getClusterId());
    Assertions.assertNotNull(returnedTableDto.getTableLocation());
    Assertions.assertFalse(returnedTableDto.isStageCreate());

    Optional<TableDto> stagedTableDto =
        openHouseInternalRepository.findById(getPrimaryKey(TableModelConstants.STAGED_TABLE_DTO));
    Assertions.assertFalse(stagedTableDto.isPresent());
  }

  @Test
  public void testMetadataUpdateForDeleted() {
    /* create the base table */
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDto returnDTO = openHouseInternalRepository.save(creationDTO);

    /* prepare tableUpdate */
    Map<String, String> baseProps = new HashMap<>(returnDTO.getTableProperties());
    baseProps.put("action", "update");
    TableDto updateDto =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(baseProps)
            .build();

    /* Using catalog to do update first. */
    TableIdentifier tableIdentifier =
        TableIdentifier.of(creationDTO.getDatabaseId(), creationDTO.getTableId());
    catalog.dropTable(tableIdentifier);

    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> openHouseInternalRepository.save(updateDto));
    Assertions.assertThrows(NoSuchTableException.class, () -> catalog.loadTable(tableIdentifier));
  }

  @Test
  public void testMetadataConcurrentUpdate() {
    /* create the base table */
    TableDto creationDTO =
        TABLE_DTO
            .toBuilder()
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    TableDto returnDTO = openHouseInternalRepository.save(creationDTO);

    /* prepare tableUpdate: Two base upon one version but different tblproperties updates are baked in. */
    Map<String, String> baseProps = new HashMap<>(returnDTO.getTableProperties());
    baseProps.put("action", "update");
    TableDto updateDtoSuccess =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(baseProps)
            .build();

    Map<String, String> basePropsFail = new HashMap<>(returnDTO.getTableProperties());
    basePropsFail.put("action", "fail");
    TableDto updateDtoFail =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(basePropsFail)
            .build();

    Assertions.assertDoesNotThrow(() -> openHouseInternalRepository.save(updateDtoSuccess));
    /* Throwing {@link CommitFailedException} in repository level, caught in service level and converted to {@link EntityConcurrentModificationException} */
    Assertions.assertThrows(
        CommitFailedException.class, () -> openHouseInternalRepository.save(updateDtoFail));
    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalRepository.deleteById(
                TableDtoPrimaryKey.builder()
                    .tableId(updateDtoSuccess.getTableId())
                    .databaseId(updateDtoSuccess.getDatabaseId())
                    .build()));
  }

  @Test
  public void testCreateTableWithTableTypeProperty() {
    for (TableType tableType : TableType.values()) {
      final String tblName = String.format("%s_%s", "tableWithType", tableType);
      TableDto tableTypeDTO =
          TABLE_DTO
              .toBuilder()
              .tableId(tblName)
              .tableType(tableType)
              .tableVersion(INITIAL_TABLE_VERSION)
              .build();

      TableDto createdDTO = openHouseInternalRepository.save(tableTypeDTO);
      Assertions.assertEquals(createdDTO.getTableId(), tblName);
      Assertions.assertEquals(createdDTO.getTableType(), tableType);

      TableDtoPrimaryKey primaryKey =
          TableDtoPrimaryKey.builder()
              .tableId(tblName)
              .databaseId(TABLE_DTO.getDatabaseId())
              .build();
      openHouseInternalRepository.deleteById(primaryKey);
      Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
    }
  }

  @Test
  void testSchemaEvolutionBasic() {
    Schema oldSchema =
        new Schema(
            required(1, "name", Types.StringType.get()),
            required(2, "count", Types.LongType.get()));
    Schema newSchema = new Schema(required(1, "name", Types.StringType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testColumnRenameIsRejectedNotSilentlyDropped() {
    // Regression guard: renaming a column (same field id, genuinely different name) must be
    // rejected loudly through save(), not silently reverted to the old name and dropped as a no-op.
    // The casing normalizer previously rewrote the new name back to the old one, so the schemas
    // compared equal and validation was skipped.
    Schema oldSchema =
        new Schema(
            required(1, "id", Types.StringType.get()), required(2, "count", Types.LongType.get()));
    Schema renamedSchema =
        new Schema(
            required(1, "user_id", Types.StringType.get()), // id=1 renamed: id -> user_id
            required(2, "count", Types.LongType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();
    TableDto createdDto = openHouseInternalRepository.save(createDto);

    TableDto renameDto =
        createdDto
            .toBuilder()
            .schema(SchemaParser.toJson(renamedSchema, false))
            .tableVersion(createdDto.getTableLocation())
            .build();

    InvalidSchemaEvolutionException thrown =
        Assertions.assertThrows(
            InvalidSchemaEvolutionException.class,
            () -> openHouseInternalRepository.save(renameDto));
    // Validate the *specific* failure: the rename is detected because the old column "id" is no
    // longer present in the new schema (it was renamed to "user_id"), not some generic error.
    Assertions.assertTrue(
        thrown.getMessage().contains("Column[id] not found in newSchema"),
        "expected the missing-renamed-column error, got: " + thrown.getMessage());

    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionWithMismatchedFieldId() {
    Schema oldSchema =
        new Schema(
            required(1, "name", Types.StringType.get()),
            required(2, "count", Types.LongType.get()));

    // negative case: Id being different
    Schema newSchema =
        new Schema(
            required(2, "name", Types.StringType.get()),
            required(1, "count", Types.LongType.get()));

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, "random_uri"));
  }

  @Test
  void testSchemaEvolutionStruct() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.StructType nestedStructType1 =
        Types.StructType.of(required(3, "leafStructCol", leafStructType1));
    Schema oldSchema = new Schema(required(4, "nestedStructCol", nestedStructType1));
    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.StructType nestedStructType2 =
        Types.StructType.of(required(3, "leafStructCol", leafStructType2));
    Schema newSchema = new Schema(required(4, "nestedStructCol", nestedStructType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionList() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.ListType nestedListType1 = Types.ListType.ofRequired(3, leafStructType1);
    Schema oldSchema = new Schema(required(4, "nestedListCol", nestedListType1));

    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.ListType nestedListType2 = Types.ListType.ofRequired(3, leafStructType2);
    Schema newSchema = new Schema(required(4, "nestedListCol", nestedListType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionMap() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.MapType nestedMapType1 =
        Types.MapType.ofRequired(3, 4, Types.StringType.get(), leafStructType1);

    Types.StructType nestedStructType1 =
        Types.StructType.of(optional(5, "nestMapCol", nestedMapType1));
    Types.MapType nested2MapType1 =
        Types.MapType.ofRequired(6, 7, Types.StringType.get(), nestedStructType1);

    Schema oldSchema = new Schema(required(8, "nested2MapCol", nested2MapType1));

    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.MapType nestedMapType2 =
        Types.MapType.ofRequired(3, 4, Types.StringType.get(), leafStructType2);

    Types.StructType nested2StructType2 =
        Types.StructType.of(optional(5, "nestMapCol", nestedMapType2));
    Types.MapType nested2MapType2 =
        Types.MapType.ofRequired(6, 7, Types.StringType.get(), nested2StructType2);

    Schema newSchema = new Schema(required(8, "nested2MapCol", nested2MapType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));

    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);
    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testReplicationFlowForPutSnapshots() {
    /* The openhouse. properties are ignored and are not validate if request is coming from replication flow */
    final String tblName = "replicaTable";
    final String replicaClusterId = "srcCluster";
    final String primaryClusterId = "destCluster";
    // existing table is of type replica_table
    Map<String, String> existingTblMap = new HashMap<>();
    existingTblMap.put("openhouse.tableType", TableType.REPLICA_TABLE.toString());
    existingTblMap.put("openhouse.clusterId", replicaClusterId);
    existingTblMap.put("openhouse.tableUri", "replicaClusterURI");
    existingTblMap.put("policies", SHARED_TABLE_POLICIES.toString());

    TableDto tableDto =
        TABLE_DTO
            .toBuilder()
            .tableId(tblName)
            .clusterId(replicaClusterId)
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableType(TableType.REPLICA_TABLE)
            .tableProperties(existingTblMap)
            .policies(SHARED_TABLE_POLICIES)
            .build();

    TableDto savedTblDto = openHouseInternalRepository.save(tableDto);
    Assertions.assertNull(savedTblDto.getPolicies().getReplication());

    Map<String, String> destTblMap = new HashMap<>();
    destTblMap.put("openhouse.tableType", TableType.PRIMARY_TABLE.toString());
    destTblMap.put("openhouse.clusterId", primaryClusterId);
    destTblMap.put("openhouse.tableUri", "primaryClusterURI");
    destTblMap.put("policies", TABLE_POLICIES.toString());

    TableDto newRequestTblDto =
        savedTblDto
            .toBuilder()
            .tableId(tblName)
            .clusterId(primaryClusterId)
            .tableType(TableType.PRIMARY_TABLE)
            .tableVersion(savedTblDto.getTableLocation())
            .tableProperties(destTblMap)
            .policies(TABLE_POLICIES)
            .build();
    // Demonstrated that the replica table updates are not blocked with table properties from
    // primary table
    TableDto newTblDTO = openHouseInternalRepository.save(newRequestTblDto);
    Assertions.assertEquals(newTblDTO.getTableId(), tblName);
    Assertions.assertEquals(newTblDTO.getTableType(), TableType.REPLICA_TABLE);
    Assertions.assertEquals(newTblDTO.getClusterId(), replicaClusterId);
    // assert that replication configs are copied as part of policies copy
    Assertions.assertEquals(
        newTblDTO.getPolicies().getReplication().getConfig().get(0).getDestination(), "CLUSTER1");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder().tableId(tblName).databaseId(TABLE_DTO.getDatabaseId()).build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  public void testRenameTableMetadataUpdate() {
    /* create the base table */
    TableDto createdDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    openHouseInternalRepository.save(createdDTO);
    /* Using catalog to do update first. */
    TableIdentifier fromTableIdentifier =
        TableIdentifier.of(createdDTO.getDatabaseId(), createdDTO.getTableId());
    TableIdentifier toTableIdentifier =
        TableIdentifier.of(createdDTO.getDatabaseId(), createdDTO.getTableId() + "_renamed");
    catalog.renameTable(fromTableIdentifier, toTableIdentifier);

    Assertions.assertTrue(
        openHouseInternalRepository
            .findById(
                TableDtoPrimaryKey.builder()
                    .databaseId(toTableIdentifier.namespace().toString())
                    .tableId(toTableIdentifier.name())
                    .build())
            .isPresent());

    Assertions.assertFalse(
        openHouseInternalRepository
            .findById(
                TableDtoPrimaryKey.builder()
                    .databaseId(fromTableIdentifier.namespace().toString())
                    .tableId(fromTableIdentifier.name())
                    .build())
            .isPresent());
  }

  @Test
  public void testRenameTablePreserveExistingCase() {
    /* create the base table */
    TableDto createdDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    openHouseInternalRepository.save(createdDTO);

    // Rename using upper case DB name
    TableIdentifier fromTableIdentifier =
        TableIdentifier.of(createdDTO.getDatabaseId(), createdDTO.getTableId());
    TableIdentifier toTableIdentifier =
        TableIdentifier.of(
            createdDTO.getDatabaseId().toUpperCase(), createdDTO.getTableId() + "_renamed");
    catalog.renameTable(fromTableIdentifier, toTableIdentifier);

    // Search with original casing on database
    Optional<TableDto> renamedTable =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder()
                .databaseId(fromTableIdentifier.namespace().toString())
                .tableId(toTableIdentifier.name())
                .build());

    Assertions.assertTrue(renamedTable.isPresent());

    // Validate metadata is storing the preserved case
    Assertions.assertEquals(renamedTable.get().getDatabaseId(), "d1");
    Assertions.assertEquals(
        renamedTable.get().getTableProperties().get("openhouse.databaseId"), "d1");
    Assertions.assertEquals(
        renamedTable.get().getTableProperties().get("openhouse.tableUri"),
        "local-cluster.d1.t1_renamed");

    // The rename destination is now guarded: an occupied destination pointer is a collision rather
    // than something a later rename silently overwrites. Leaving d1.t1_renamed behind would
    // therefore collide with other tests in this class, which share one Spring context.
    openHouseInternalRepository.deleteById(
        TableDtoPrimaryKey.builder().databaseId("d1").tableId("t1_renamed").build());
  }

  @Test
  public void testDefaultFileFormatWithFeatureToggle() {
    final String ENABLE_DEFAULT_FILE_FORMAT = "enableDefaultFileFormat";
    final String CLUSTER_DEFAULT_FORMAT = "orc"; // This should match the cluster default
    final String USER_PROVIDED_FORMAT = "parquet";

    // Scenario 1: DB does NOT have toggle enabled, user provides DEFAULT_FILE_FORMAT
    // Expected: User's value should be ignored, cluster default should be used
    Map<String, String> userPropsWithFormat = new HashMap<>();
    userPropsWithFormat.put(TableProperties.DEFAULT_FILE_FORMAT, USER_PROVIDED_FORMAT);

    TableDto tableDto1 =
        TABLE_DTO
            .toBuilder()
            .tableId("test_toggle_disabled")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(userPropsWithFormat)
            .build();

    PreservedKeyChecker targetPreservedKeyChecker =
        AopTestUtils.getUltimateTargetObject(preservedKeyChecker);

    // Mock the preservedKeyChecker to simulate toggle disabled (don't allow user override)
    Mockito.doReturn(true)
        .when(targetPreservedKeyChecker)
        .isKeyPreservedForTable(Mockito.eq(TableProperties.DEFAULT_FILE_FORMAT), Mockito.any());
    Mockito.doReturn(false)
        .when(targetPreservedKeyChecker)
        .allowKeyInCreation(Mockito.eq(TableProperties.DEFAULT_FILE_FORMAT), Mockito.any());

    TableDto createdDto1 = openHouseInternalRepository.save(tableDto1);

    // Should use cluster default, not user provided value
    Assertions.assertEquals(
        CLUSTER_DEFAULT_FORMAT.toLowerCase(),
        createdDto1.getTableProperties().get(TableProperties.DEFAULT_FILE_FORMAT).toLowerCase());

    // Scenario 2: DB DOES have toggle enabled, user provides DEFAULT_FILE_FORMAT
    // Expected: User's value should override cluster default
    TableDto tableDto2 =
        TABLE_DTO
            .toBuilder()
            .tableId("test_toggle_enabled")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(userPropsWithFormat)
            .build();

    // Mock the preservedKeyChecker to simulate toggle enabled (allow user override)
    Mockito.doReturn(false)
        .when(targetPreservedKeyChecker)
        .isKeyPreservedForTable(Mockito.eq(TableProperties.DEFAULT_FILE_FORMAT), Mockito.any());

    TableDto createdDto2 = openHouseInternalRepository.save(tableDto2);

    // Should use user provided value when toggle is enabled
    Assertions.assertEquals(
        USER_PROVIDED_FORMAT,
        createdDto2.getTableProperties().get(TableProperties.DEFAULT_FILE_FORMAT));

    // Scenario 3: User does not provide DEFAULT_FILE_FORMAT at all
    // Expected: Cluster default should be used regardless of toggle state
    Map<String, String> userPropsWithoutFormat = new HashMap<>();
    userPropsWithoutFormat.put("someOtherProperty", "someValue");

    TableDto tableDto3 =
        TABLE_DTO
            .toBuilder()
            .tableId("test_no_format_provided")
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(userPropsWithoutFormat)
            .build();

    TableDto createdDto3 = openHouseInternalRepository.save(tableDto3);

    // Should use cluster default when no value provided
    Assertions.assertEquals(
        CLUSTER_DEFAULT_FORMAT.toLowerCase(),
        createdDto3.getTableProperties().get(TableProperties.DEFAULT_FILE_FORMAT).toLowerCase());

    // Clean up test tables
    TableDtoPrimaryKey key1 =
        TableDtoPrimaryKey.builder()
            .tableId("test_toggle_disabled")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    TableDtoPrimaryKey key2 =
        TableDtoPrimaryKey.builder()
            .tableId("test_toggle_enabled")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    TableDtoPrimaryKey key3 =
        TableDtoPrimaryKey.builder()
            .tableId("test_no_format_provided")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();

    openHouseInternalRepository.deleteById(key1);
    openHouseInternalRepository.deleteById(key2);
    openHouseInternalRepository.deleteById(key3);

    Assertions.assertFalse(openHouseInternalRepository.existsById(key1));
    Assertions.assertFalse(openHouseInternalRepository.existsById(key2));
    Assertions.assertFalse(openHouseInternalRepository.existsById(key3));
  }

  // ===== Case-insensitive write normalization =====

  @Test
  void testCaseInsensitiveWrite_succeeds_andPreservesTableCasing() {
    // Table is created with uppercase column name "ID".
    // A subsequent save with lowercase "id" (same field id) should succeed and leave
    // the stored schema using the original "ID" casing.
    Schema tableSchema =
        new Schema(
            required(1, "ID", Types.StringType.get()), optional(2, "value", Types.LongType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("case_insensitive_write_test")
            .schema(SchemaParser.toJson(tableSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    TableDto savedDto = openHouseInternalRepository.save(createDto);

    // Writer submits schema with lowercase "id" (same field id=1) — simulates a case-insensitive
    // Spark/Trino write that sends column names in a different casing than the table.
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "value", Types.LongType.get()));

    TableDto updateDto =
        savedDto
            .toBuilder()
            .schema(SchemaParser.toJson(writeSchema, false))
            .tableVersion(savedDto.getTableLocation())
            .build();

    // Save should succeed without throwing InvalidSchemaEvolutionException
    TableDto updatedDto =
        Assertions.assertDoesNotThrow(
            () -> openHouseInternalRepository.save(updateDto),
            "save() with differently-cased column names should succeed");

    // The stored schema must preserve the original table casing ("ID", not "id")
    Schema storedSchema = SchemaParser.fromJson(updatedDto.getSchema());
    Assertions.assertEquals(
        "ID",
        storedSchema.findField(1).name(),
        "Table casing must be preserved after a case-insensitive write");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .tableId("case_insensitive_write_test")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testCaseInsensitiveWrite_blockedForCaseDuplicateTable() {
    // A table with case-duplicate columns (both "id" and "ID") must NOT apply normalization.
    // A write with mismatched casing on such a table should still throw.
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "ID", Types.StringType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("case_duplicate_write_test")
            .schema(SchemaParser.toJson(tableSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    TableDto savedDto = openHouseInternalRepository.save(createDto);

    // Writer sends "Id" for field id=1 (table has "id") — casing mismatch on a case-dup table
    Schema writeSchema =
        new Schema(
            required(1, "Id", Types.StringType.get()), optional(2, "ID", Types.StringType.get()));

    TableDto updateDto =
        savedDto
            .toBuilder()
            .schema(SchemaParser.toJson(writeSchema, false))
            .tableVersion(savedDto.getTableLocation())
            .build();

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> openHouseInternalRepository.save(updateDto),
        "save() with mismatched casing on a case-duplicate table must throw");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .tableId("case_duplicate_write_test")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testCaseInsensitiveWrite_succeedsWithColumnAddition_andPreservesTableCasing() {
    // Path B: writer has wrong casing on an existing column AND adds a new column.
    // Normalization must fix the existing column casing first; then validateWriteSchema
    // sees a valid evolution (existing IDs intact, new column appended) and must accept it.
    Schema tableSchema =
        new Schema(
            required(1, "ID", Types.StringType.get()), optional(2, "value", Types.LongType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("case_insensitive_evolution_test")
            .schema(SchemaParser.toJson(tableSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    TableDto savedDto = openHouseInternalRepository.save(createDto);

    // Writer submits wrong casing on "ID" (sends "id") and also adds new column "new_col" (id=3).
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.StringType.get()),
            optional(2, "value", Types.LongType.get()),
            optional(3, "new_col", Types.LongType.get()));

    TableDto updateDto =
        savedDto
            .toBuilder()
            .schema(SchemaParser.toJson(writeSchema, false))
            .tableVersion(savedDto.getTableLocation())
            .build();

    // After normalization "id" → "ID", sameSchema is false (new_col added), so
    // validateWriteSchema is invoked and must accept the valid column addition.
    TableDto updatedDto =
        Assertions.assertDoesNotThrow(
            () -> openHouseInternalRepository.save(updateDto),
            "save() with casing mismatch + new column should succeed");

    Schema storedSchema = SchemaParser.fromJson(updatedDto.getSchema());
    Assertions.assertEquals(
        "ID", storedSchema.findField(1).name(), "Existing column casing must be preserved");
    Assertions.assertNotNull(
        storedSchema.findField(3), "New column must be present in stored schema");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .tableId("case_insensitive_evolution_test")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testCaseInsensitiveWrite_caseDuplicateTable_succeedsWithExactCasing() {
    // Path D: table has case-duplicate columns — normalization guard skips normalization.
    // A write with exactly matching casing must still succeed (sameSchema = true).
    // This verifies the guard does not break legitimate writes to legacy case-duplicate tables.
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "ID", Types.StringType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableId("case_duplicate_exact_write_test")
            .schema(SchemaParser.toJson(tableSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    TableDto savedDto = openHouseInternalRepository.save(createDto);

    // Write with exact same casing as the table — normalization is skipped but sameSchema = true.
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "ID", Types.StringType.get()));

    TableDto updateDto =
        savedDto
            .toBuilder()
            .schema(SchemaParser.toJson(writeSchema, false))
            .tableVersion(savedDto.getTableLocation())
            .build();

    Assertions.assertDoesNotThrow(
        () -> openHouseInternalRepository.save(updateDto),
        "save() with exact casing on a case-duplicate table must succeed");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .tableId("case_duplicate_exact_write_test")
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  private TableDtoPrimaryKey getPrimaryKey(TableDto tableDto) {
    return TableDtoPrimaryKey.builder()
        .databaseId(tableDto.getDatabaseId())
        .tableId(tableDto.getTableId())
        .build();
  }

  private HouseTablePrimaryKey getHouseTablePrimaryKey(TableDtoPrimaryKey primaryKey) {
    return HouseTablePrimaryKey.builder()
        .databaseId(primaryKey.getDatabaseId())
        .tableId(primaryKey.getTableId())
        .build();
  }

  private void verifyTable(TableDto table) {
    Assertions.assertEquals(TABLE_DTO.getTableId(), table.getTableId());
    Assertions.assertEquals(TABLE_DTO.getDatabaseId(), table.getDatabaseId());
    Assertions.assertEquals(TABLE_DTO.getClusterId(), table.getClusterId());
    Assertions.assertEquals(TABLE_DTO.getTableUri(), table.getTableUri());
    Path path =
        Paths.get(
            "file:",
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            table.getDatabaseId(),
            table.getTableId() + "-" + table.getTableUUID());
    Assertions.assertEquals(TABLE_DTO.getTimePartitioning(), table.getTimePartitioning());
    Assertions.assertEquals(TABLE_DTO.getClustering(), table.getClustering());
    Assertions.assertTrue(table.getTableLocation().startsWith(path.toString()));
  }

  private void verifyTable(HouseTable table) {
    Assertions.assertEquals(TABLE_DTO.getTableId(), table.getTableId());
    Assertions.assertEquals(TABLE_DTO.getDatabaseId(), table.getDatabaseId());
    Assertions.assertEquals(TABLE_DTO.getClusterId(), table.getClusterId());
    Assertions.assertEquals(TABLE_DTO.getTableUri(), table.getTableUri());
    Path path =
        Paths.get(
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            table.getDatabaseId(),
            table.getTableId() + "-" + table.getTableUUID());
    Assertions.assertTrue(table.getTableLocation().startsWith(path.toString()));
  }

  // ---------------------------------------------------------------------------------------------
  // Table listings must exclude views in the query, never by post-filtering a Page
  // ---------------------------------------------------------------------------------------------

  /**
   * Canonical interleaved fixture: four visible tables (two legacy NULL, two explicit TABLE)
   * interleaved with three VIEW rows. A fetch-then-filter implementation returns a SHORT first page
   * (1 row) with totalElements=7/totalPages=4; the correct pre-pagination predicate returns a full
   * 2-row page with totalElements=4/totalPages=2.
   */
  private static final String ENTITY_TYPE_DB = "entity_type_db";

  private static final List<String> CANONICAL_TABLE_IDS =
      Arrays.asList("t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit");

  private static final List<String> CANONICAL_VIEW_IDS =
      Arrays.asList("t01_view", "t03_view", "t05_view");

  private static final String CASE_DB = "entity_type_case_db";

  private HouseTable rawPointer(String databaseId, String tableId, String entityType) {
    return HouseTable.builder()
        .databaseId(databaseId)
        .tableId(tableId)
        .clusterId("test-cluster")
        .tableUri(String.format("test-cluster.%s.%s", databaseId, tableId))
        .tableUUID(UUID.randomUUID().toString())
        .tableLocation(String.format("/base/%s/%s-uuid/00001-x.metadata.json", databaseId, tableId))
        .tableVersion(INITIAL_TABLE_VERSION)
        .entityType(entityType)
        .build();
  }

  /** Seeds raw pointer rows and returns their keys so the caller can delete them in a finally. */
  private List<HouseTablePrimaryKey> seedRawPointers(String databaseId, String[][] idAndType) {
    List<HouseTablePrimaryKey> keys = new ArrayList<>();
    for (String[] entry : idAndType) {
      houseTablesRepository.save(rawPointer(databaseId, entry[0], entry[1]));
      keys.add(HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(entry[0]).build());
    }
    return keys;
  }

  private List<HouseTablePrimaryKey> seedCanonicalPointers(String databaseId) {
    return seedRawPointers(
        databaseId,
        new String[][] {
          {"t00_legacy", null},
          {"t01_view", "VIEW"},
          {"t02_explicit", "TABLE"},
          {"t03_view", "VIEW"},
          {"t04_legacy", null},
          {"t05_view", "VIEW"},
          {"t06_explicit", "TABLE"}
        });
  }

  private List<HouseTablePrimaryKey> seedCaseNormalizationPointers(String databaseId) {
    return seedRawPointers(
        databaseId,
        new String[][] {
          {"case00_null", null},
          {"case01_upper_table", "TABLE"},
          {"case02_lower_table", "table"},
          {"case03_mixed_table", "TaBlE"},
          {"case04_upper_view", "VIEW"},
          {"case05_lower_view", "view"},
          {"case06_mixed_view", "ViEw"},
          {"case07_garbage", "UNKNOWN"}
        });
  }

  /**
   * Raw pointer rows are invisible to the table APIs by design, so no table-API cleanup can remove
   * them. Every test that seeds them MUST delete them explicitly, otherwise later tests in this
   * class (which asserts exact database/table sets, and shares one Spring context across methods)
   * are polluted.
   */
  private void deleteRawPointers(List<HouseTablePrimaryKey> keys) {
    for (HouseTablePrimaryKey key : keys) {
      try {
        houseTablesRepository.deleteById(key);
      } catch (Exception e) {
        // Best effort: a missing row must not mask the real assertion failure.
      }
    }
  }

  private static OpenHouseInternalCatalog openHouseCatalog(Catalog catalog) {
    return (OpenHouseInternalCatalog) AopTestUtils.getUltimateTargetObject(catalog);
  }

  private static List<String> identifierNames(List<TableIdentifier> identifiers) {
    return identifiers.stream().map(TableIdentifier::name).sorted().collect(Collectors.toList());
  }

  private static Pageable sortedPage(int page) {
    return PageRequest.of(page, 2, Sort.by("tableId"));
  }

  /** SHOW TABLES contract: a view never appears in the catalog's table listing. */
  @Test
  public void testCatalogListTablesExcludesViewsAndKeepsNullAndTable() {
    List<HouseTablePrimaryKey> keys = seedCanonicalPointers(ENTITY_TYPE_DB);
    try {
      List<TableIdentifier> identifiers = catalog.listTables(Namespace.of(ENTITY_TYPE_DB));

      Assertions.assertEquals(CANONICAL_TABLE_IDS, identifierNames(identifiers));
      Assertions.assertTrue(
          identifierNames(identifiers).stream().noneMatch(CANONICAL_VIEW_IDS::contains),
          "No VIEW row may appear in SHOW TABLES: " + identifierNames(identifiers));
    } finally {
      deleteRawPointers(keys);
    }
  }

  /** Anti-post-filter assertion for the paginated catalog listing overload. */
  @Test
  public void testCatalogListTablesFiltersBeforePagination() {
    List<HouseTablePrimaryKey> keys = seedCanonicalPointers(ENTITY_TYPE_DB);
    try {
      OpenHouseInternalCatalog ohCatalog = openHouseCatalog(catalog);

      Page<TableIdentifier> page0 =
          ohCatalog.listTables(Namespace.of(ENTITY_TYPE_DB), sortedPage(0));
      Assertions.assertEquals(4, page0.getTotalElements());
      Assertions.assertEquals(2, page0.getTotalPages());
      Assertions.assertEquals(2, page0.getContent().size());
      Assertions.assertEquals(
          Arrays.asList("t00_legacy", "t02_explicit"),
          page0.getContent().stream().map(TableIdentifier::name).collect(Collectors.toList()));

      Page<TableIdentifier> page1 =
          ohCatalog.listTables(Namespace.of(ENTITY_TYPE_DB), sortedPage(1));
      Assertions.assertEquals(4, page1.getTotalElements());
      Assertions.assertEquals(2, page1.getTotalPages());
      Assertions.assertEquals(2, page1.getContent().size());
      Assertions.assertEquals(
          Arrays.asList("t04_legacy", "t06_explicit"),
          page1.getContent().stream().map(TableIdentifier::name).collect(Collectors.toList()));
    } finally {
      deleteRawPointers(keys);
    }
  }

  /** Anti-post-filter assertion for the HouseTable-preserving paginated listing. */
  @Test
  public void testListHouseTablesFiltersBeforePagination() {
    List<HouseTablePrimaryKey> keys = seedCanonicalPointers(ENTITY_TYPE_DB);
    try {
      OpenHouseInternalCatalog ohCatalog = openHouseCatalog(catalog);

      Page<HouseTable> page0 =
          ohCatalog.listHouseTables(Namespace.of(ENTITY_TYPE_DB), sortedPage(0));
      Assertions.assertEquals(4, page0.getTotalElements());
      Assertions.assertEquals(2, page0.getTotalPages());
      Assertions.assertEquals(2, page0.getContent().size());
      Assertions.assertEquals(
          Arrays.asList("t00_legacy", "t02_explicit"),
          page0.getContent().stream().map(HouseTable::getTableId).collect(Collectors.toList()));
      Assertions.assertTrue(
          page0.getContent().stream().noneMatch(h -> "VIEW".equalsIgnoreCase(h.getEntityType())));

      Page<HouseTable> page1 =
          ohCatalog.listHouseTables(Namespace.of(ENTITY_TYPE_DB), sortedPage(1));
      Assertions.assertEquals(4, page1.getTotalElements());
      Assertions.assertEquals(2, page1.getTotalPages());
      Assertions.assertEquals(2, page1.getContent().size());
      Assertions.assertEquals(
          Arrays.asList("t04_legacy", "t06_explicit"),
          page1.getContent().stream().map(HouseTable::getTableId).collect(Collectors.toList()));
    } finally {
      deleteRawPointers(keys);
    }
  }

  /** All three {@code searchTables} overloads must filter identically and before paging. */
  @Test
  public void testOpenHouseRepositorySearchTablesFiltersAllOverloads() {
    List<HouseTablePrimaryKey> keys = seedCanonicalPointers(ENTITY_TYPE_DB);
    try {
      List<TableDto> plain = openHouseInternalRepository.searchTables(ENTITY_TYPE_DB);
      Assertions.assertEquals(
          CANONICAL_TABLE_IDS,
          plain.stream().map(TableDto::getTableId).sorted().collect(Collectors.toList()));

      Page<TableDto> page0 =
          openHouseInternalRepository.searchTables(ENTITY_TYPE_DB, sortedPage(0));
      Assertions.assertEquals(4, page0.getTotalElements());
      Assertions.assertEquals(2, page0.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("t00_legacy", "t02_explicit"),
          page0.getContent().stream().map(TableDto::getTableId).collect(Collectors.toList()));

      Page<TableDto> page1 =
          openHouseInternalRepository.searchTables(ENTITY_TYPE_DB, sortedPage(1));
      Assertions.assertEquals(4, page1.getTotalElements());
      Assertions.assertEquals(2, page1.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("t04_legacy", "t06_explicit"),
          page1.getContent().stream().map(TableDto::getTableId).collect(Collectors.toList()));

      // The fields projection goes through listHouseTables, so it must filter identically and
      // still populate the requested field.
      Page<TableDto> fieldsPage0 =
          openHouseInternalRepository.searchTables(
              ENTITY_TYPE_DB, sortedPage(0), Collections.singletonList("tableLocation"));
      Assertions.assertEquals(4, fieldsPage0.getTotalElements());
      Assertions.assertEquals(2, fieldsPage0.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("t00_legacy", "t02_explicit"),
          fieldsPage0.getContent().stream().map(TableDto::getTableId).collect(Collectors.toList()));
      Assertions.assertTrue(
          fieldsPage0.getContent().stream().allMatch(dto -> dto.getTableLocation() != null),
          "fields=tableLocation must be projected for every returned table");
    } finally {
      deleteRawPointers(keys);
    }
  }

  /**
   * Database enumeration: a database whose only pointer is a view must disappear entirely. Note the
   * global-scope precondition — {@code findAllIds} is not database-scoped, so this test asserts the
   * pointer table is empty first to keep a failure here diagnosable as leakage rather than as a
   * filtering bug.
   */
  @Test
  public void testFindAllIdsExcludesViewOnlyDatabases() {
    Assertions.assertTrue(
        Streams.stream(houseTablesRepository.findAll()).count() == 0,
        "This test asserts global pointer counts and requires a clean pointer table; "
            + "a previous test leaked rows");

    List<HouseTablePrimaryKey> keys = new ArrayList<>();
    try {
      keys.addAll(seedRawPointers("db00_legacy", new String[][] {{"t1", null}}));
      keys.addAll(seedRawPointers("db01_view_only", new String[][] {{"t1", "VIEW"}}));
      keys.addAll(seedRawPointers("db02_explicit", new String[][] {{"t1", "TABLE"}}));
      keys.addAll(seedRawPointers("db03_view_only", new String[][] {{"t1", "VIEW"}}));
      keys.addAll(seedRawPointers("db04_legacy", new String[][] {{"t1", null}}));
      keys.addAll(seedRawPointers("db05_view_only", new String[][] {{"t1", "VIEW"}}));
      keys.addAll(seedRawPointers("db06_explicit", new String[][] {{"t1", "TABLE"}}));

      List<String> databaseIds =
          openHouseInternalRepository.findAllIds().stream()
              .map(TableDtoPrimaryKey::getDatabaseId)
              .sorted()
              .collect(Collectors.toList());
      Assertions.assertEquals(
          Arrays.asList("db00_legacy", "db02_explicit", "db04_legacy", "db06_explicit"),
          databaseIds);

      Pageable dbPage = PageRequest.of(0, 2, Sort.by("databaseId"));
      Page<TableDtoPrimaryKey> page0 = openHouseInternalRepository.findAllIds(dbPage);
      Assertions.assertEquals(4, page0.getTotalElements());
      Assertions.assertEquals(2, page0.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("db00_legacy", "db02_explicit"),
          page0.getContent().stream()
              .map(TableDtoPrimaryKey::getDatabaseId)
              .collect(Collectors.toList()));

      Page<TableDtoPrimaryKey> page1 =
          openHouseInternalRepository.findAllIds(PageRequest.of(1, 2, Sort.by("databaseId")));
      Assertions.assertEquals(4, page1.getTotalElements());
      Assertions.assertEquals(2, page1.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("db04_legacy", "db06_explicit"),
          page1.getContent().stream()
              .map(TableDtoPrimaryKey::getDatabaseId)
              .collect(Collectors.toList()));
    } finally {
      deleteRawPointers(keys);
    }
  }

  /**
   * Case/garbage matrix at the internal H2 query layer.
   *
   * <p>H2 (MODE=MySQL) is case-SENSITIVE while production MySQL's default collation is not, so this
   * proves the query normalizes explicitly (e.g. {@code upper(h.entityType) = 'TABLE'}) rather than
   * relying on the provider's collation — a bare {@code = 'TABLE'} comparison would hide the
   * lower/mixed-case table rows here and fail. It does NOT certify production MySQL behavior; the
   * authoritative case contract lives in the Java guards ({@code
   * HouseTableTest#testEntityTypeClassification} and the catalog guard tests).
   */
  @Test
  public void testCaseInsensitiveTypePredicateAndGarbageFailClosed() {
    List<HouseTablePrimaryKey> keys = seedCaseNormalizationPointers(CASE_DB);
    try {
      List<String> expectedVisible =
          Arrays.asList(
              "case00_null", "case01_upper_table", "case02_lower_table", "case03_mixed_table");
      List<String> expectedHidden =
          Arrays.asList(
              "case04_upper_view", "case05_lower_view", "case06_mixed_view", "case07_garbage");

      List<String> listed = identifierNames(catalog.listTables(Namespace.of(CASE_DB)));
      Assertions.assertEquals(expectedVisible, listed);
      Assertions.assertTrue(
          listed.stream().noneMatch(expectedHidden::contains),
          "Views (any spelling) and unknown types must fail closed out of SHOW TABLES: " + listed);

      OpenHouseInternalCatalog ohCatalog = openHouseCatalog(catalog);

      Page<TableIdentifier> page0 = ohCatalog.listTables(Namespace.of(CASE_DB), sortedPage(0));
      Assertions.assertEquals(4, page0.getTotalElements());
      Assertions.assertEquals(2, page0.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("case00_null", "case01_upper_table"),
          page0.getContent().stream().map(TableIdentifier::name).collect(Collectors.toList()));

      Page<HouseTable> housePage0 = ohCatalog.listHouseTables(Namespace.of(CASE_DB), sortedPage(0));
      Assertions.assertEquals(4, housePage0.getTotalElements());
      Assertions.assertEquals(2, housePage0.getTotalPages());
      Assertions.assertEquals(
          Arrays.asList("case00_null", "case01_upper_table"),
          housePage0.getContent().stream()
              .map(HouseTable::getTableId)
              .collect(Collectors.toList()));

      Page<HouseTable> housePage1 = ohCatalog.listHouseTables(Namespace.of(CASE_DB), sortedPage(1));
      Assertions.assertEquals(
          Arrays.asList("case02_lower_table", "case03_mixed_table"),
          housePage1.getContent().stream()
              .map(HouseTable::getTableId)
              .collect(Collectors.toList()));

      // Hidden, not dropped: the raw rows are all still there.
      for (HouseTablePrimaryKey key : keys) {
        Assertions.assertTrue(
            houseTablesRepository.findById(key).isPresent(),
            "Raw pointer " + key.getTableId() + " must still exist; it is hidden, not deleted");
      }
    } finally {
      deleteRawPointers(keys);
    }
  }
}
