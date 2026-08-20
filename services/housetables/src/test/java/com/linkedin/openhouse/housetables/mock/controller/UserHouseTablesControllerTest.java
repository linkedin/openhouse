package com.linkedin.openhouse.housetables.mock.controller;

import static com.linkedin.openhouse.housetables.model.ServiceAuditModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.mock.MockUserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
public class UserHouseTablesControllerTest {

  @Autowired private MockMvc mvc;

  @Autowired private ApplicationContext applicationContext;

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptor;

  @Test
  public void testApiHandler() {
    Assertions.assertEquals(
        applicationContext.getBean(UserTableHtsApiHandler.class).getClass(),
        MockUserTableHtsApiHandler.class);
  }

  /** @throws Exception */
  @Test
  public void testGetTableRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", TEST_TABLE_ID)
                .param("tableId", TEST_DB_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testPutTableRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(PUT_USER_TABLE_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TEST_GET_USER_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testServiceAuditSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put("/hts/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(PUT_USER_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(serviceAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_PUT_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testRenameTableRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .contentType(MediaType.APPLICATION_JSON)
                .param("fromDatabaseId", TEST_TABLE_ID)
                .param("fromTableId", TEST_DB_ID)
                .param("toDatabaseId", "newTableName")
                .param("toTableId", "newDatabaseName")
                .param("metadataLocation", "mockMetadataLocation")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNoContent());
  }

  /**
   * The rename route owns the type it operates on. The controller binds it before dispatch, so the
   * handler never has to infer it and no caller can supply it.
   */
  @Test
  public void testRenameThreadsTheControllerOwnedTableType() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .contentType(MediaType.APPLICATION_JSON)
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", TEST_TABLE_ID)
                .param("toDatabaseId", TEST_DB_ID)
                .param("toTableId", TEST_TABLE_ID + "_renamed")
                .param("metadataLocation", "mockMetadataLocation")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNoContent());

    Assertions.assertEquals(EntityType.TABLE, mockHandler().getLastRenameEntityType());
  }

  /** The neutral occupancy route is wired to its own handler method, not to the table read. */
  @Test
  public void testGetNeutralEntityRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_NEUTRAL_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content().json(TestHtsApiConstants.TEST_GET_NEUTRAL_ENTITY_RESPONSE_BODY.toJson()));
  }

  /** The view point read is a distinct route bound to a distinct handler method. */
  @Test
  public void testGetViewRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_VIEW_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY.toJson()));
  }

  /** Both view query routes, plain and paged, reach the view list handler methods. */
  @Test
  public void testGetViewRows() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .param("databaseId", TEST_DB_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content().json(TestHtsApiConstants.TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY.toJson()));

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .param("databaseId", TEST_DB_ID)
                .param("page", "0")
                .param("size", "50")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON));
  }

  /**
   * The view PUT is its own route and its own handler method, and the endpoint — not the payload —
   * is what fixes the type: a body that omits the field still reaches the handler stamped VIEW.
   */
  @Test
  public void testPutViewRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(PUT_USER_VIEW_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY.toJson()));

    Assertions.assertEquals("VIEW", mockHandler().getLastPutView().getEntityType());
  }

  /** The table PUT stamps the canonical constant on a payload that omitted it. */
  @Test
  public void testPutTableRowStampsCanonicalTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(TEST_USER_TABLE.toBuilder().entityType(null).build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    Assertions.assertEquals("TABLE", mockHandler().getLastPutEntity().getEntityType());
  }

  /** The view DELETE is a distinct route from the table one and answers 204. */
  @Test
  public void testDeleteViewRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_VIEW_ID))
        .andExpect(status().isNoContent());
  }

  private MockUserTableHtsApiHandler mockHandler() {
    return (MockUserTableHtsApiHandler) applicationContext.getBean(UserTableHtsApiHandler.class);
  }
}
