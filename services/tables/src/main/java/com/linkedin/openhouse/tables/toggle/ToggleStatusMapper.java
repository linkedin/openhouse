package com.linkedin.openhouse.tables.toggle;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ToggleStatusMapper {

  @Mapping(source = "toggleStatus.status", target = "toggleStatusEnum")
  TableToggleStatus toTableToggleStatus(ToggleStatusKey key, ToggleStatus toggleStatus);
}
