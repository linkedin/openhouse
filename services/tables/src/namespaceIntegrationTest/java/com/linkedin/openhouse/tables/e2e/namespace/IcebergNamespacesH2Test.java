package com.linkedin.openhouse.tables.e2e.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.HouseTablesSpringApplication;
import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepositoryImpl;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import com.linkedin.openhouse.tables.exception.NamespaceExceptionHandler;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.services.IcebergNamespacesService;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Exercises Tables HTTP translation through the generated HTS client and a live H2 service. */
@SpringBootTest(
    classes = HouseTablesSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "cluster.housetables.database.type=IN_MEMORY",
      "cluster.housetables.database.url=jdbc:h2:mem:namespace_roundtrip;MODE=MYSQL;DB_CLOSE_DELAY=-1",
      "cluster.storage.root-path=build/namespace-storage",
      "spring.jpa.hibernate.ddl-auto=none",
      "spring.sql.init.mode=always",
      "spring.jpa.defer-datasource-initialization=true",
      "spring.autoconfigure.exclude="
          + "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration,"
          + "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration"
    })
@DirtiesContext
class IcebergNamespacesH2Test {
  @LocalServerPort private int port;
  @Autowired private DataSource dataSource;
  private MockMvc mvc;

  @BeforeEach
  void setUp() {
    ApiClient client = new ApiClient();
    client.setBasePath("http://localhost:" + port);
    AuthorizationHandler authorizationHandler = mock(AuthorizationHandler.class);
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
    AuthorizationUtils authorizationUtils = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorizationUtils, "authorizationHandler", authorizationHandler);
    mvc =
        MockMvcBuilders.standaloneSetup(
                new IcebergNamespacesController(
                    new IcebergNamespacesService(
                        new HouseDatabaseRepositoryImpl(
                            new DatabaseApi(client), new UserTableApi(client)),
                        authorizationUtils)))
            .setControllerAdvice(new NamespaceExceptionHandler())
            .setMessageConverters(new MappingJackson2HttpMessageConverter())
            .build();
  }

  @Test
  void createLoadSparseUpdateAndDropRoundTrip() throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespace\":[\"roundtrip\"],\"properties\":{\"a\":\"1\",\"b\":\"2\"}}"))
        .andExpect(status().isOk());
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"ROUNDTRIP\"]}"))
        .andExpect(status().isConflict());
    mvc.perform(
            post("/v1/foo/namespaces/ROUNDTRIP/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"c\":\"3\"},\"removals\":[\"b\",\"d\"]}"))
        .andExpect(status().isOk())
        .andExpect(
            content().json("{\"updated\":[\"c\"],\"removed\":[\"b\"],\"missing\":[\"d\"]}", true));
    mvc.perform(get("/v1/foo/namespaces/ROUNDTRIP"))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .json(
                    "{\"namespace\":[\"roundtrip\"],\"properties\":{\"a\":\"1\",\"c\":\"3\"}}",
                    true));
    mvc.perform(delete("/v1/foo/namespaces/roundtrip")).andExpect(status().isNoContent());
    mvc.perform(get("/v1/foo/namespaces/roundtrip")).andExpect(status().isNotFound());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "VIEW"})
  void occupiedNamespacesKeepMetadataAndEntityRows(String entityType) throws Exception {
    String namespace = "occupied_" + entityType;
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"" + namespace + "\"]}"))
        .andExpect(status().isOk());
    JdbcTemplate database = new JdbcTemplate(dataSource);
    database.update(
        "INSERT INTO user_table_row (database_id, table_id, version, metadata_location, entity_type)"
            + " VALUES (?, 'foo', 0, 'file:///unread-metadata', ?)",
        namespace,
        entityType);
    mvc.perform(delete("/v1/foo/namespaces/" + namespace))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("NamespaceNotEmptyException"));
    mvc.perform(get("/v1/foo/namespaces/" + namespace)).andExpect(status().isOk());
    assertEquals(
        1,
        database.queryForObject(
            "SELECT COUNT(*) FROM user_table_row WHERE database_id = ?", Integer.class, namespace));
  }

  @Test
  void legacyDatabaseWithTablesButWithoutMetadataIsMissing() throws Exception {
    JdbcTemplate database = new JdbcTemplate(dataSource);
    database.update(
        "INSERT INTO user_table_row (database_id, table_id, version) VALUES ('legacy', 'foo', 0)");
    mvc.perform(delete("/v1/foo/namespaces/legacy")).andExpect(status().isNotFound());
    assertEquals(
        1,
        database.queryForObject(
            "SELECT COUNT(*) FROM user_table_row WHERE database_id = 'legacy'", Integer.class));
  }
}
