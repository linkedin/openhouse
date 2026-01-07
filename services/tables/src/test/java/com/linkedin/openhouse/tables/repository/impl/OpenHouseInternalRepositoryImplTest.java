package com.linkedin.openhouse.tables.repository.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OpenHouseInternalRepositoryImplTest {

  @Mock private PoliciesSpecMapper policiesMapper;
  @Mock private MeterRegistry meterRegistry;
  @Mock private ClusterProperties clusterProperties;
  @Mock private PreservedKeyChecker preservedKeyChecker;

  @InjectMocks private OpenHouseInternalRepositoryImpl openHouseInternalRepository;

  private static final String DB_ID = "db";
  private static final String TABLE_ID = "table";
  private static final String SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"string\"}]}";

  @BeforeEach
  void setUp() {
    when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));
    when(preservedKeyChecker.allowKeyInCreation(anyString(), any())).thenReturn(true);
    when(policiesMapper.toPoliciesJsonString(any())).thenReturn("{}");
    when(clusterProperties.getClusterIcebergWriteFormatDefault()).thenReturn("parquet");
  }

  @Test
  void testComputePropsForTableCreation_DefaultMetadataVersions() {
    int clusterDefaultMaxMetadataVersions = 10;
    when(clusterProperties.getClusterIcebergWriteMetadataPreviousVersionsMax())
        .thenReturn(clusterDefaultMaxMetadataVersions);
    when(clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled())
        .thenReturn(true);
    when(clusterProperties.getClusterIcebergFormatVersion()).thenReturn(2);

    TableDto tableDto = createTableDto(new HashMap<>());
    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals(
        String.valueOf(clusterDefaultMaxMetadataVersions),
        actualProps.get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX));
  }

  @Test
  void testComputePropsForTableCreation_UserProvidedMetadataVersions() {
    int clusterDefaultMaxMetadataVersions = 10;
    String userProvidedMaxMetadataVersions = "5";

    // Although cluster property is mocked, it shouldn't be used for the key
    when(clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled())
        .thenReturn(true);
    when(clusterProperties.getClusterIcebergFormatVersion()).thenReturn(2);

    Map<String, String> userProps = new HashMap<>();
    userProps.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, userProvidedMaxMetadataVersions);
    TableDto tableDto = createTableDto(userProps);

    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals(
        userProvidedMaxMetadataVersions,
        actualProps.get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX));
  }

  private TableDto createTableDto(Map<String, String> properties) {
    return TableDto.builder()
        .databaseId(DB_ID)
        .tableId(TABLE_ID)
        .schema(SCHEMA_JSON)
        .tableProperties(properties)
        .tableVersion("v1")
        .tableType(TableType.PRIMARY_TABLE)
        .build();
  }
}
