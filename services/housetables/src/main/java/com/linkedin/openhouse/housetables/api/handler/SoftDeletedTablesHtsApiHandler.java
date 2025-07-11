package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.SoftDeletedUserTableKey;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;

public interface SoftDeletedTablesHtsApiHandler
    extends HouseTablesApiHandler<SoftDeletedUserTableKey, UserTable> {

  ApiResponse<EntityResponseBody<UserTable>> recoverEntity(
      SoftDeletedUserTableKey softDeletedUserTable);
}
