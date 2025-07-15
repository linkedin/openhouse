package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.time.Instant;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface SoftDeletedUserTablesMapper {

  SoftDeletedUserTableRow toSoftDeletedUserTableRow(UserTableRow userTableRow);

  UserTableDto toUserTableDto(SoftDeletedUserTableRow softDeletedUserTableRow);

  @AfterMapping
  default void calculateDeleteTimeAndTimeToLive(
      @MappingTarget
          SoftDeletedUserTableRow.SoftDeletedUserTableRowBuilder softDeletedUserTableRowBuilder) {
    Instant deleteTime = Instant.now();
    softDeletedUserTableRowBuilder.deletedAtMs(deleteTime.toEpochMilli());

    // TODO: This should be a configurable value based off of database-level configurations
    softDeletedUserTableRowBuilder.purgeAfterMs(
        deleteTime.plus(7, java.time.temporal.ChronoUnit.DAYS).toEpochMilli());
  }
}
