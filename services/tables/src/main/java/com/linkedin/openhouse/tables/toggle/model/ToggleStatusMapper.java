package com.linkedin.openhouse.tables.toggle.model;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ToggleStatusMapper {

  @Mapping(source = "toggleStatus.status", target = "toggleStatusEnum")
  public abstract TableToggleStatus toTableToggleStatus(
      ToggleStatusKey key, ToggleStatus toggleStatus);
}
