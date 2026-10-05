package com.linkedin.openhouse.tables.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TablesServiceIdentityTest {
  @Mock private OpenHouseInternalRepository repository;
  @Mock private AuthorizationUtils authorizationUtils;

  @InjectMocks private TablesServiceImpl tablesService;

  @Test
  void getTableByIdentityLoadsAndAuthorizesCurrentCatalogRow() {
    TableDto reference =
        TableDto.builder()
            .databaseId("renamed-db")
            .tableId("renamed-table")
            .clusterId("destination")
            .tableUUID("destination-uuid")
            .creationTime(20L)
            .build();
    TableDto currentTable =
        reference.toBuilder().schema("{}").tableVersion("metadata-version").build();
    TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId("renamed-db").tableId("renamed-table").build();
    when(repository.findTableRefsByIdentity("destination", "destination-uuid", 20L))
        .thenReturn(List.of(reference));
    when(repository.findById(key)).thenReturn(Optional.of(currentTable));

    TableDto result =
        tablesService.getTableByIdentity("destination", "destination-uuid", 20L, "replicator");

    assertThat(result).isSameAs(currentTable);
    verify(authorizationUtils)
        .checkTablePrivilege(currentTable, "replicator", Privileges.GET_TABLE_METADATA);
  }
}
