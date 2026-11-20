package com.linkedin.openhouse.tables.mock.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import com.linkedin.openhouse.tables.exception.NamespaceExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
@Import(NamespaceExceptionHandler.class)
class IcebergNamespacesControllerTest {
  @Autowired private MockMvc mvc;
  @Autowired private IcebergNamespacesController controller;
  @MockBean private HouseNamespaceRepository repository;
  @MockBean private AuthorizationHandler authorizationHandler;

  @BeforeEach
  void setUp() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
  }

  @AfterEach
  void clearAuthentication() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void fourRoutesMatchIcebergBodiesAndSuccessStatuses() throws Exception {
    NamespaceMetadata metadata =
        new NamespaceMetadata("private", "foo", Collections.singletonMap("a", "1"), "1");
    when(repository.create(eq("foo"), any())).thenReturn(metadata);
    mvc.perform(
            post("/v1/catalog_name/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"a\":\"1\"}}"))
        .andExpect(status().isOk())
        .andExpect(content().json("{\"namespace\":[\"foo\"],\"properties\":{\"a\":\"1\"}}", true));
    when(repository.getByName("foo")).thenReturn(metadata);
    mvc.perform(get("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespace[0]").value("foo"))
        .andExpect(jsonPath("$.namespaceUuid").doesNotExist());
    mvc.perform(
            post("/v1/catalog_name/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"b\":\"2\"},\"removals\":[\"a\",\"c\"]}"))
        .andExpect(status().isOk())
        .andExpect(
            content().json("{\"updated\":[\"b\"],\"removed\":[\"a\"],\"missing\":[\"c\"]}", true));
    mvc.perform(delete("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
    verify(repository).delete("foo");
  }

  @Test
  void databaseWritersWithoutSystemAdminCannotCreateOrChangeProperties() throws Exception {
    when(authorizationHandler.checkAccessDecision(
            anyString(), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    mvc.perform(
            post("/v1/catalog_name/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.error.type").value("ForbiddenException"));
    mvc.perform(
            post("/v1/catalog_name/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"a\":\"1\"}}"))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/v1/catalog_name/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"removals\":[\"a\"]}"))
        .andExpect(status().isForbidden());
  }

  @Test
  void authorizationOutageUsesGenericFailureResponse() throws Exception {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(new AuthorizationServiceException("private upstream failure"));
    mvc.perform(get("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.code").value(500))
        .andExpect(jsonPath("$.error.type").value("RESTException"))
        .andExpect(jsonPath("$.error.message").value("Namespace operation failed"));
  }

  @Test
  void readsAllowMetadataPrivilegeButDropRequiresSystemAdmin() throws Exception {
    when(authorizationHandler.checkAccessDecision(
            anyString(), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Collections.emptyMap(), "1"));
    mvc.perform(get("/v1/catalog_name/namespaces/foo")).andExpect(status().isOk());
    mvc.perform(delete("/v1/catalog_name/namespaces/foo")).andExpect(status().isForbidden());
    verify(authorizationHandler)
        .checkAccessDecision(
            anyString(), any(DatabaseDto.class), eq(Privileges.GET_TABLE_METADATA));
    verify(authorizationHandler)
        .checkAccessDecision(anyString(), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
    mvc.perform(get("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespace[0]").value("foo"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{",
        "{}",
        "{\"namespace\":[]}",
        "{\"namespace\":[\"foo\",\"bar\"]}",
        "{\"namespace\":[null]}",
        "{\"namespace\":[\"foo.bar\"]}",
        "{\"namespace\":[\"foo\"],\"properties\":null}",
        "{\"namespace\":[\"foo\"],\"properties\":{\"a\":null}}"
      })
  void invalidBodiesUseSafeIcebergErrors(String body) throws Exception {
    mvc.perform(post("/v1/foo/namespaces").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400))
        .andExpect(jsonPath("$.error.type").value("BadRequestException"))
        .andExpect(jsonPath("$.stackTrace").doesNotExist())
        .andExpect(jsonPath("$.error.stack").doesNotExist());
  }

  @Test
  void invalidCreateReportsNamespaceWithoutPropertyValues() throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo.bar\"],\"properties\":{\"foo\":\"bar_secret\"}}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.message").value(containsString("foo.bar")))
        .andExpect(jsonPath("$.error.message").value(not(containsString("bar_secret"))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"\\ud800", "\\udc00", "foo\\ud800bar"})
  void malformedPropertyKeysFailBeforeMetadataAccess(String key) throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"" + key + "\":\"value\"}}"))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"" + key + "\":\"value\"}}"))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"removals\":[\"" + key + "\"]}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void supplementaryPropertyKeysPassPublicValidation() throws Exception {
    when(repository.create("foo", Collections.singletonMap("😀", "value")))
        .thenReturn(
            new NamespaceMetadata(
                "identifier", "foo", Collections.singletonMap("😀", "value"), "1"));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"\\ud83d\\ude00\":\"value\"}}"))
        .andExpect(status().isOk());
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of("😀", "value"), "1"));
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"removals\":[\"\\ud83d\\ude00\"]}"))
        .andExpect(status().isOk());
  }

  @Test
  void omittedCollectionsAreEmptyAndOverlapIsUnprocessable() throws Exception {
    when(repository.create("foo", Collections.emptyMap()))
        .thenReturn(new NamespaceMetadata("private", "foo", Collections.emptyMap(), "1"));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isOk());
    verify(repository).create("foo", Collections.emptyMap());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"a\":\"1\"},\"removals\":[\"a\"]}"))
        .andExpect(status().isUnprocessableEntity())
        .andExpect(jsonPath("$.error.code").value(422));
  }

  @Test
  void namespaceSeparatorAndOverlengthAreRejectedOnCreate() throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\\u001fbar\"]}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespace\":[\"" + String.join("", Collections.nCopies(129, "a")) + "\"]}"))
        .andExpect(status().isBadRequest());
  }

  @ParameterizedTest
  @ValueSource(strings = {"updates", "removals", "neither"})
  void omittedUpdateCollectionsPreserveSuppliedChanges(String supplied) throws Exception {
    Map<String, String> updates = "updates".equals(supplied) ? Map.of("a", "1") : Map.of();
    Set<String> removals = "removals".equals(supplied) ? Set.of("b") : Set.of();
    Map<String, Object> request =
        "updates".equals(supplied)
            ? Map.of("updates", updates)
            : "removals".equals(supplied) ? Map.of("removals", removals) : Map.of();
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of("b", "2"), "1"));
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .json(
                    new ObjectMapper()
                        .writeValueAsString(
                            Map.of(
                                "updated",
                                updates.keySet(),
                                "removed",
                                removals,
                                "missing",
                                List.of())),
                    true));
    Map<String, String> expected =
        "updates".equals(supplied)
            ? Map.of("a", "1", "b", "2")
            : "removals".equals(supplied) ? Map.of() : Map.of("b", "2");
    verify(repository).update("foo", new NamespaceMetadata("identifier", "foo", expected, "1"));
  }

  @Test
  void duplicateCreateRetainsConflictResponse() throws Exception {
    when(repository.create("foo", Map.of()))
        .thenThrow(new DuplicateCreationException("Namespace already exists"));
    mvc.perform(
            post("/v1/foo/namespaces/")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("AlreadyExistsException"));
  }

  @Test
  void failedUpdateUsesGenericFailureResponse() throws Exception {
    when(repository.getByName("namespaces"))
        .thenReturn(new NamespaceMetadata("identifier", "namespaces", Map.of(), "1"));
    when(repository.update(eq("namespaces"), any()))
        .thenThrow(new RuntimeException("Namespace was modified concurrently"));
    mvc.perform(
            post("/v1/foo/namespaces/namespaces/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.type").value("RESTException"));
  }

  @Test
  void updateAndDeleteConflictsUseCommitFailedResponse() throws Exception {
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of(), "1"));
    when(repository.update(eq("foo"), any()))
        .thenThrow(new EntityConcurrentModificationException("foo", null));
    doThrow(new EntityConcurrentModificationException("foo", null)).when(repository).delete("foo");
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
    verify(repository, times(1)).getByName("foo");
    verify(repository, times(1)).update(eq("foo"), any());
    mvc.perform(delete("/v1/foo/namespaces/foo"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
  }

  @Test
  void failedDeleteUsesGenericFailureResponse() throws Exception {
    doThrow(new RuntimeException("Namespace was modified concurrently"))
        .when(repository)
        .delete("namespaces");
    mvc.perform(delete("/v1/foo/namespaces/namespaces"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.type").value("RESTException"));
  }

  @Test
  void failedCreateUsesSafeGenericResponse() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenThrow(
            new RuntimeException("secret property value", new IllegalStateException("secret")));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isInternalServerError())
        .andExpect(
            content()
                .json(
                    "{\"error\":{\"message\":\"Namespace operation failed\",\"type\":\"RESTException\",\"code\":500}}",
                    true));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updates\":null}",
        "{\"removals\":null}",
        "{\"updates\":{\"a\":null}}",
        "{\"removals\":[null]}"
      })
  void invalidPropertyUpdatesAreRejected(String body) throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400));
  }

  @Test
  void verifiedNamespaceAbsenceRetainsNotFoundResponse() throws Exception {
    when(repository.getByName("foo")).thenThrow(new NotFoundException("Namespace does not exist"));
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.type").value("NoSuchNamespaceException"));
  }

  @Test
  void failedReadUsesSafeGenericResponse() throws Exception {
    doThrow(
            new RuntimeException(
                "Namespace metadata service unavailable", new IllegalStateException("secret")))
        .when(repository)
        .getByName("foo");
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.code").value(500))
        .andExpect(jsonPath("$.error.type").value("RESTException"))
        .andExpect(content().string(not(containsString("secret"))));
  }

  @Test
  void databaseAuthorizationRejectsReadsBeforeLookup() throws Exception {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.error.code").value(403));
  }

  @Test
  void methodGuardRejectsUnauthenticatedCallers() {
    SecurityContextHolder.clearContext();
    assertThrows(AccessDeniedException.class, () -> controller.loadNamespaceMetadata("foo", "bar"));
  }
}
