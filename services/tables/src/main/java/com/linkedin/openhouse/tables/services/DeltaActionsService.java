package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v1.request.DeltaActionsRequestBody;
import com.linkedin.openhouse.tables.model.TableDto;

public interface DeltaActionsService {

  TableDto appendDeltaActions(
      String databaseId,
      String tableId,
      DeltaActionsRequestBody deltaActionsRequestBody,
      String tableCreator);
}
