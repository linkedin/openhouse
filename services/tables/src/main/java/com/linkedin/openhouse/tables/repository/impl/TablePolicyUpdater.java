package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import org.apache.iceberg.UpdateProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Handles updates to table policies by serializing them and storing them to table properties
 * Extensions of this class can implement more granular updates to policies by examining tableDto
 * class
 */
@Component
public class TablePolicyUpdater {

  @Autowired protected PoliciesSpecMapper policiesMapper;

  /**
   * Default policy update behavior that preserves existing functionality. Updates all policies if
   * they differ from the existing ones.
   *
   * @param updateProperties The properties update builder
   * @param tableDto The DTO containing new policy values
   * @param existingTableProps Current table properties
   * @return true if any policies were updated
   */
  public boolean updatePoliciesIfNeeded(
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
