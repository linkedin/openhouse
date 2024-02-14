package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import org.mapstruct.Mapper;

/** Mapper class to transform between DTO and Data Model objects. */
@Mapper(componentModel = "spring")
public interface DatabasesMapper {
  /**
   * From a source {@link DatabaseDto}, prepare a {@link
   * com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody}
   *
   * @param databaseDto Source {@link DatabaseDto} to transform.
   * @return Destination {@link
   *     com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody} to be forwarded
   *     to the client.
   */
  GetDatabaseResponseBody toGetDatabaseResponseBody(DatabaseDto databaseDto);

  /** Construct a {@link DatabaseDto} using databaseId and clusterId */
  DatabaseDto toDatabaseDto(String databaseId, String clusterId);
}
