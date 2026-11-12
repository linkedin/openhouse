package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceUnavailableException;
import com.linkedin.openhouse.tables.services.NamespacesService;
import com.linkedin.openhouse.tables.services.TablesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@WithMockUser(username = "testUser")
class NamespaceTableIsolationTest {
  @Autowired private MockMvc mvc;
  @Autowired private StorageManager storageManager;
  @Autowired private ApplicationContext applicationContext;
  @MockBean private HouseNamespaceRepository namespaceRepository;

  @Test
  void tableAndNamespaceInterfacesResolveToTheSameService() {
    assertSame(
        applicationContext.getBean(TablesService.class),
        applicationContext.getBean(NamespacesService.class));
  }

  @Test
  void legacyTableLifecycleAndListingNeverReadNamespaceMetadata() throws Exception {
    when(namespaceRepository.findByName(anyString()))
        .thenThrow(new HouseNamespaceUnavailableException());
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    mvc.perform(
            get(
                "/v1/databases/{database}/tables/{table}",
                GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                GET_TABLE_RESPONSE_BODY.getTableId()))
        .andExpect(status().isOk());
    mvc.perform(get("/v1/databases")).andExpect(status().isOk());
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }
}
