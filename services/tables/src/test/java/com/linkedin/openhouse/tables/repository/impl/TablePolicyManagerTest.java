package com.linkedin.openhouse.tables.repository.impl;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.UpdateProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TablePolicyManagerTest {

  @Mock private PoliciesSpecMapper policiesMapper;

  @InjectMocks private TablePolicyManager tablePolicyManager;

  @Test
  void testManagePoliciesOnUpdateIfNeeded_newPolicies() {
    UpdateProperties updateProperties = mock(UpdateProperties.class);
    when(updateProperties.set(anyString(), anyString())).thenReturn(updateProperties);

    String incomingPolicies = "{\"retention\":{\"count\":30,\"granularity\":\"DAY\"}}";
    when(policiesMapper.toPoliciesJsonString(any())).thenReturn(incomingPolicies);

    TableDto tableDto = TableDto.builder().databaseId("db").tableId("table").build();

    Map<String, String> existingProps = new HashMap<>();

    boolean updated =
        tablePolicyManager.managePoliciesOnUpdateIfNeeded(
            updateProperties, tableDto, existingProps);

    Assertions.assertTrue(updated);
    verify(updateProperties).set(InternalRepositoryUtils.POLICIES_KEY, incomingPolicies);
  }

  @Test
  void testManagePoliciesOnUpdateIfNeeded_samePoliciesNoUpdate() {
    UpdateProperties updateProperties = mock(UpdateProperties.class);

    String policiesJson = "{\"retention\":{\"count\":30,\"granularity\":\"DAY\"}}";
    when(policiesMapper.toPoliciesJsonString(any())).thenReturn(policiesJson);

    TableDto tableDto = TableDto.builder().databaseId("db").tableId("table").build();

    Map<String, String> existingProps = new HashMap<>();
    existingProps.put(InternalRepositoryUtils.POLICIES_KEY, policiesJson);

    boolean updated =
        tablePolicyManager.managePoliciesOnUpdateIfNeeded(
            updateProperties, tableDto, existingProps);

    Assertions.assertFalse(updated);
    verify(updateProperties, never()).set(anyString(), anyString());
  }

  @Test
  void testManagePoliciesOnUpdateIfNeeded_differentPoliciesUpdated() {
    UpdateProperties updateProperties = mock(UpdateProperties.class);
    when(updateProperties.set(anyString(), anyString())).thenReturn(updateProperties);

    String newPolicies = "{\"retention\":{\"count\":60,\"granularity\":\"DAY\"}}";
    String existingPolicies = "{\"retention\":{\"count\":30,\"granularity\":\"DAY\"}}";
    when(policiesMapper.toPoliciesJsonString(any())).thenReturn(newPolicies);

    TableDto tableDto = TableDto.builder().databaseId("db").tableId("table").build();

    Map<String, String> existingProps = new HashMap<>();
    existingProps.put(InternalRepositoryUtils.POLICIES_KEY, existingPolicies);

    boolean updated =
        tablePolicyManager.managePoliciesOnUpdateIfNeeded(
            updateProperties, tableDto, existingProps);

    Assertions.assertTrue(updated);
    verify(updateProperties).set(InternalRepositoryUtils.POLICIES_KEY, newPolicies);
  }
}
