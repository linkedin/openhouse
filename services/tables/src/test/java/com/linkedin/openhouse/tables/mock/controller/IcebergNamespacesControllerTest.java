package com.linkedin.openhouse.tables.mock.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.internal.catalog.model.DatabaseMetadata;
import com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.config.NamespaceErrorFilter;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import com.linkedin.openhouse.tables.exception.NamespaceExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletResponse;
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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
@Import(NamespaceExceptionHandler.class)
class IcebergNamespacesControllerTest {
  @Autowired private MockMvc mvc;
  @Autowired private IcebergNamespacesController controller;
  @MockBean private HouseDatabaseRepository repository;
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
    DatabaseMetadata metadata =
        new DatabaseMetadata("private", "foo", Collections.singletonMap("a", "1"));
    when(repository.create(eq("foo"), any())).thenReturn(metadata);
    mvc.perform(
            post("/v1/catalog_name/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"a\":\"1\"}}"))
        .andExpect(status().isOk())
        .andExpect(content().json("{\"namespace\":[\"foo\"],\"properties\":{\"a\":\"1\"}}", true));
    when(repository.findByName("foo")).thenReturn(Optional.of(metadata));
    mvc.perform(get("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespace[0]").value("foo"))
        .andExpect(jsonPath("$.databaseUuid").doesNotExist());
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new DatabasePropertyUpdate(
                Collections.singletonList("b"),
                Collections.singletonList("a"),
                Collections.singletonList("c")));
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{",
        "{}",
        "{\"namespace\":[]}",
        "{\"namespace\":[\"foo\",\"bar\"]}",
        "{\"namespace\":[null]}",
        "{\"namespace\":[1]}",
        "{\"namespace\":[\"foo.bar\"]}",
        "{\"namespace\":[\"foo\"],\"properties\":null}",
        "{\"namespace\":[\"foo\"],\"properties\":{\"a\":null}}",
        "{\"namespace\":[\"foo\"],\"properties\":{\"a\":1}}"
      })
  void invalidBodiesUseSafeIcebergErrors(String body) throws Exception {
    mvc.perform(post("/v1/foo/namespaces").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400))
        .andExpect(jsonPath("$.error.type").value("BadRequestException"))
        .andExpect(jsonPath("$.stackTrace").doesNotExist())
        .andExpect(jsonPath("$.error.stack").doesNotExist());
    verifyNoInteractions(repository);
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
    verifyNoInteractions(repository);
  }

  @Test
  void supplementaryPropertyKeysPassPublicValidation() throws Exception {
    when(repository.create("foo", Collections.singletonMap("😀", "value")))
        .thenReturn(
            new DatabaseMetadata("identifier", "foo", Collections.singletonMap("😀", "value")));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"\\ud83d\\ude00\":\"value\"}}"))
        .andExpect(status().isOk());
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new DatabasePropertyUpdate(
                Collections.emptyList(), Collections.singletonList("😀"), Collections.emptyList()));
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"removals\":[\"\\ud83d\\ude00\"]}"))
        .andExpect(status().isOk());
  }

  @Test
  void omittedCollectionsAreEmptyAndOverlapIsUnprocessable() throws Exception {
    when(repository.create("foo", Collections.emptyMap()))
        .thenReturn(new DatabaseMetadata("private", "foo", Collections.emptyMap()));
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
    verify(repository, never()).updateProperties(anyString(), any(), any());
  }

  @Test
  void namespaceSeparatorAndOverlengthAreRejected() throws Exception {
    mvc.perform(get("/v1/foo/namespaces/{namespace}", "foo\u001fbar"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400));
    mvc.perform(
            get("/v1/foo/namespaces/{namespace}", String.join("", Collections.nCopies(129, "a"))))
        .andExpect(status().isBadRequest());
    verifyNoInteractions(repository);
  }

  @ParameterizedTest
  @ValueSource(strings = {"updates", "removals", "neither"})
  void omittedUpdateCollectionsPreserveSuppliedChanges(String supplied) throws Exception {
    Map<String, String> updates = supplied.equals("updates") ? Map.of("a", "1") : Map.of();
    Set<String> removals = supplied.equals("removals") ? Set.of("b") : Set.of();
    Map<String, Object> request =
        supplied.equals("updates")
            ? Map.of("updates", updates)
            : supplied.equals("removals") ? Map.of("removals", removals) : Map.of();
    when(repository.updateProperties("foo", updates, removals))
        .thenReturn(
            new DatabasePropertyUpdate(
                List.copyOf(updates.keySet()), List.copyOf(removals), List.of()));
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
    verify(repository).updateProperties("foo", updates, removals);
  }

  @ParameterizedTest
  @ValueSource(strings = {"a", "😀"})
  void propertyKeyLimitUsesUtf16Units(String character) throws Exception {
    String key = character.repeat(512 / character.length());
    String oversizedKey = key + "a";
    when(repository.create("foo", Map.of(key, "one")))
        .thenReturn(new DatabaseMetadata("identifier", "foo", Map.of(key, "one")));
    when(repository.updateProperties("foo", Map.of(key, "two"), Set.of()))
        .thenReturn(new DatabasePropertyUpdate(List.of(key), List.of(), List.of()));
    when(repository.updateProperties("foo", Map.of(), Set.of(key)))
        .thenReturn(new DatabasePropertyUpdate(List.of(), List.of(key), List.of()));
    ObjectMapper mapper = new ObjectMapper();
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    mapper.writeValueAsString(
                        Map.of("namespace", List.of("foo"), "properties", Map.of(key, "one")))))
        .andExpect(status().isOk());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(Map.of("updates", Map.of(key, "two")))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.updated[0]").value(key));
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(Map.of("removals", List.of(key)))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.removed[0]").value(key));
    verify(repository).create("foo", Map.of(key, "one"));
    verify(repository).updateProperties("foo", Map.of(key, "two"), Set.of());
    verify(repository).updateProperties("foo", Map.of(), Set.of(key));
    clearInvocations(repository);
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    mapper.writeValueAsString(
                        Map.of(
                            "namespace",
                            List.of("foo"),
                            "properties",
                            Map.of(oversizedKey, "one")))))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(Map.of("updates", Map.of(oversizedKey, "two")))))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(Map.of("removals", List.of(oversizedKey)))))
        .andExpect(status().isBadRequest());
    verifyNoInteractions(repository);
  }

  @Test
  void conflictsUseOperationReasonsForEveryAcceptedRoute() throws Exception {
    when(repository.create("foo", Map.of()))
        .thenThrow(new HouseDatabaseException(Reason.ALREADY_EXISTS));
    mvc.perform(
            post("/v1/foo/namespaces/")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("AlreadyExistsException"));
    when(repository.updateProperties("namespaces", Map.of(), Set.of()))
        .thenThrow(new HouseDatabaseException(Reason.CONCURRENT_MODIFICATION));
    mvc.perform(
            post("/v1/foo/namespaces/namespaces/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
    when(repository.findByName("namespaces"))
        .thenReturn(Optional.of(new DatabaseMetadata("identifier", "namespaces", Map.of())));
    doThrow(new HouseDatabaseException(Reason.CONCURRENT_MODIFICATION))
        .when(repository)
        .delete("namespaces");
    mvc.perform(delete("/v1/foo/namespaces/namespaces"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
  }

  @Test
  void uncertainMutationUsesReconciliationErrorRatherThanUnavailable() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenThrow(new HouseDatabaseException(Reason.UNKNOWN_OUTCOME));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.type").value("CommitStateUnknownException"))
        .andExpect(
            jsonPath("$.error.message")
                .value(
                    "The mutation outcome is unknown. Reload namespace metadata and reconcile before retrying."));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updates\":null}", "{\"removals\":null}",
        "{\"updates\":{\"a\":1}}", "{\"removals\":[null]}"
      })
  void invalidPropertyUpdatesAreRejected(String body) throws Exception {
    mvc.perform(
            post("/v1/foo/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400));
    verifyNoInteractions(repository);
  }

  @Test
  void upstreamStatusesRemainDistinctAndMessagesStayPrivate() throws Exception {
    when(repository.findByName("foo")).thenReturn(Optional.empty());
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.type").value("NoSuchNamespaceException"));
    when(repository.findByName("foo"))
        .thenThrow(
            new HouseDatabaseException(Reason.UNAVAILABLE, new IllegalStateException("secret")));
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.error.code").value(503))
        .andExpect(
            content()
                .string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("secret"))));
    when(repository.create(anyString(), any()))
        .thenThrow(new HouseDatabaseException(Reason.ALREADY_EXISTS));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("AlreadyExistsException"));
  }

  @Test
  void databaseAuthorizationRejectsReadsBeforeLookup() throws Exception {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.error.code").value(403));
    verifyNoInteractions(repository);
  }

  @Test
  void actualTokenInterceptorAndMethodGuardRejectUnauthenticatedCallers() throws Exception {
    mvc.perform(get("/v1/foo/namespaces/foo").header("Authorization", ""))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.error.code").value(401));
    mvc.perform(get("/v1/foo/namespaces/foo").header("Authorization", "Bearer foo"))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.error.type").value("NotAuthorizedException"));
    SecurityContextHolder.clearContext();
    assertThrows(AccessDeniedException.class, () -> controller.loadNamespaceMetadata("foo", "bar"));
    verifyNoInteractions(repository);
  }

  @Test
  void preMvcSecurityErrorsHaveIcebergShapeAndHideFilterMessages() throws Exception {
    Filter rejection =
        (request, response, chain) -> ((HttpServletResponse) response).sendError(403, "secret");
    MockMvc filtered =
        MockMvcBuilders.standaloneSetup(controller)
            .addFilters(new NamespaceErrorFilter(new ObjectMapper()), rejection)
            .build();
    filtered
        .perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isForbidden())
        .andExpect(
            content()
                .json(
                    "{\"error\":{\"message\":\"Namespace request failed\",\"type\":\"ForbiddenException\",\"code\":403}}",
                    true));
    filtered
        .perform(get("/v1/databases"))
        .andExpect(status().isForbidden())
        .andExpect(content().string(""));
    verifyNoInteractions(repository);
  }
}
