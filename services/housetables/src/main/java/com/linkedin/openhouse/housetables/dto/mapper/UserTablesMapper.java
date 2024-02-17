package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRowPrimaryKey;
import java.util.Map;
import java.util.Optional;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

/**
 * Mapper class to transform between {@link
 * com.linkedin.openhouse.housetables.dto.model.UserTableDto} and Data Model objects.
 *
 * <p>Each endpoints of House table services will injects their own mapper to create isolation
 * between each other.
 */
@Mapper(
    componentModel = "spring",
    uses = {UserTableVersionMapper.class})
public interface UserTablesMapper {

  /**
   * From a source {@link UserTable}, prepare a {@link UserTableRow} object which is compliant with
   * the storage {@link
   * com.linkedin.openhouse.housetables.repository.impl.iceberg.UserTableHtsRepository},
   * particularly wrt its {@link Long} version requirements.
   *
   * @param userTable userTable source to be mapped
   * @param existingUserTableRow if not present, set "version" as 1. If present, set "version" as
   *     the last saved version as long as: existingUserTableRow's metadata location and userTable's
   *     version match.
   * @return Destination House Table Data Model for storing Table Metadata.
   */
  @Mapping(target = "version", source = "userTable", qualifiedByName = "toVersion")
  UserTableRow toUserTableRow(
      UserTable userTable, @Context Optional<UserTableRow> existingUserTableRow);

  /**
   * From a source Table Metadata from House Table, prepare a User Table DTO object.
   *
   * @param userTableRow Source Data Model object for User Table Metadata in House Table.
   * @return Destination User Table DTO to be forwarded to the controller.
   */
  @Mapping(target = "tableVersion", source = "metadataLocation")
  UserTableDto toUserTableDto(UserTableRow userTableRow);

  /**
   * From a source DTO object {@link UserTableDto}, prepare {@link UserTable} as part of recipe to
   * construct {@link EntityResponseBody}
   *
   * @param userTableDto
   * @return
   */
  UserTable toUserTable(UserTableDto userTableDto);

  /**
   * From a source {@link UserTable} object, prepare a {@link UserTableDto}.
   *
   * @param userTable
   * @return
   */
  UserTableDto fromUserTable(UserTable userTable);

  /**
   * From a source {@link UserTable} object prepare a {@link UserTableRowPrimaryKey} from
   * corresponding fields. Note that {@link UserTableKey} is a different POJO as {@link
   * UserTableRowPrimaryKey}, in which the latter deals with repository layer.
   *
   * @param userTable
   * @return
   */
  UserTableRowPrimaryKey fromUserTableToRowKey(UserTable userTable);

  /** Mapping a Properties Map to {@link UserTable} */
  UserTable mapToUserTable(Map<String, String> properties);

  /** Map a {@link UserTableRow} to its Primary Key */
  UserTableRowPrimaryKey fromUserTableRowToRowKey(UserTableRow userTableRow);

  /** Map a {@link UserTableRow} to a {@link UserTableIcebergRow} */
  UserTableIcebergRow toUserTableIcebergRow(UserTableRow userTableRow);

  /** Map a {@link UserTableRowPrimaryKey} to a {@link UserTableIcebergRowPrimaryKey} */
  UserTableIcebergRowPrimaryKey toUserTableIcebergRowPrimaryKey(
      UserTableRowPrimaryKey userTableRowPrimaryKey);

  /** Map a {@link UserTableIcebergRow} to a {@link UserTableRow} */
  UserTableRow toUserTableRow(UserTableIcebergRow userTableIcebergRow);
}
