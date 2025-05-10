package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.TableToggleStatusKey;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatus;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.services.ToggleStatusesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * {@link ToggleStatusesApiHandler} is essentially read only. Thus, any write API are not
 * implemented here.
 */
@Component
public class OpenHouseToggleStatusesApiHandler implements ToggleStatusesApiHandler {
  @Autowired private ToggleStatusesService toggleStatusesService;

  @Override
  public ApiResponse<EntityResponseBody<ToggleStatus>> getEntity(TableToggleStatusKey key) {
    return ApiResponse.<EntityResponseBody<ToggleStatus>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            EntityResponseBody.<ToggleStatus>builder()
                .entity(
                    toggleStatusesService.getTableToggleStatus(
                        key.getFeatureId(), key.getDatabaseId(), key.getTableId()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<ToggleStatus>> getEntities(ToggleStatus entity) {
    throw new UnsupportedOperationException("Get all toggle status is unsupported");
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<ToggleStatus>> getEntities(
      ToggleStatus entity, int page, int size, String sortBy) {
    throw new UnsupportedOperationException("Get all toggle status is unsupported");
  }

  @Override
  public ApiResponse<Void> deleteEntity(TableToggleStatusKey key) {
    throw new UnsupportedOperationException("Delete toggle status is unsupported");
  }

  @Override
  public ApiResponse<EntityResponseBody<ToggleStatus>> putEntity(ToggleStatus entity) {
    throw new UnsupportedOperationException("Update toggle status is unsupported");
  }
}
