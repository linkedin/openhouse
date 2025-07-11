package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.Named;

@Mapper(componentModel = "spring")
public interface SoftDeletedUserTablesMapper {

  SoftDeletedUserTableRow toSoftDeletedUserTableRow(UserTableRow userTableRow);

  @Mapping(
      target = "timeToLive",
      source = "timeToLive",
      qualifiedByName = "timeStampToLocalDateTimeUtc")
  UserTableDto toUserTableDto(SoftDeletedUserTableRow softDeletedUserTableRow);

  @AfterMapping
  default void calculateDeleteTimeAndTimeToLive(
      @MappingTarget
          SoftDeletedUserTableRow.SoftDeletedUserTableRowBuilder softDeletedUserTableRowBuilder) {
    Instant deleteTime = Instant.now(Clock.systemUTC());
    softDeletedUserTableRowBuilder.deletedAtMs(deleteTime.toEpochMilli());

    // TODO: This should be a configurable value based off of database-level configurations
    softDeletedUserTableRowBuilder.timeToLive(
        Timestamp.from(deleteTime.plus(7, java.time.temporal.ChronoUnit.DAYS)));
  }

  @Named("timeStampToLocalDateTimeUtc")
  default java.time.LocalDateTime convertTimeStampToLocalDateTimeUtc(Timestamp timestamp) {
    return timestamp.toInstant().atZone(java.time.ZoneOffset.UTC).toLocalDateTime();
  }
}
