package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import org.apache.iceberg.UpdateProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Manages table policies by serializing them and storing them to table properties. Extensions of
 * this class can implement more granular policy management by examining tableDto class, for example
 * registering policies with an external policy store during table creation.
 */
@Component
public class TablePolicyManager {

  @Autowired protected PoliciesSpecMapper policiesMapper;

  /**
   * Hook for policy registration during table creation. Called before the Iceberg table is created.
   * Default is no-op since policies are already serialized into Iceberg table properties via
   * computePropsForTableCreation(). Extensions can override to register policies with external
   * stores.
   *
   * @param tableDto The DTO containing policy values for the new table
   */
  public void managePoliciesOnCreateIfNeeded(TableDto tableDto) {
    // no-op in OSS; policies are already in Iceberg table properties
  }

  /**
   * Default policy update behavior that preserves existing functionality. Updates all policies if
   * they differ from the existing ones.
   *
   * @param updateProperties The properties update builder
   * @param tableDto The DTO containing new policy values
   * @param existingTableProps Current table properties
   * @return true if any policies were updated
   */
  public boolean managePoliciesOnUpdateIfNeeded(
      UpdateProperties updateProperties,
      TableDto tableDto,
      Map<String, String> existingTableProps) {

    boolean policiesUpdated;

    String tableDtoPolicyString = policiesMapper.toPoliciesJsonString(tableDto);
    if (!existingTableProps.containsKey(InternalRepositoryUtils.POLICIES_KEY)) {
      updateProperties.set(InternalRepositoryUtils.POLICIES_KEY, tableDtoPolicyString);
      policiesUpdated = true;
    } else {
      String policiesJsonString = existingTableProps.get(InternalRepositoryUtils.POLICIES_KEY);
      policiesUpdated =
          alterPoliciesIfNeeded(updateProperties, tableDtoPolicyString, policiesJsonString);
    }

    return policiesUpdated;
  }

  /**
   * @param updateProperties
   * @param policiesFromRequest
   * @param policiesFromTable
   * @return True if alteration of policies occurred.
   */
  protected boolean alterPoliciesIfNeeded(
      UpdateProperties updateProperties, String policiesFromRequest, String policiesFromTable) {
    boolean policiesUpdated = false;
    if (!policiesFromRequest.equals(policiesFromTable)) {
      updateProperties.set(InternalRepositoryUtils.POLICIES_KEY, policiesFromRequest);
      policiesUpdated = true;
    }

    return policiesUpdated;
  }
}
