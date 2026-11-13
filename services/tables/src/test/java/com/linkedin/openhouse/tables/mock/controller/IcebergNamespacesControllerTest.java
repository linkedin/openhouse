package com.linkedin.openhouse.tables.mock.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.exception.checked.ServiceFailureException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.common.exception.checked.VersionConflictException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.config.NamespaceErrorFilter;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import com.linkedin.openhouse.tables.exception.NamespaceExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.springframework.security.access.AuthorizationServiceException;
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
        new NamespaceMetadata("private", "foo", Collections.singletonMap("a", "1"));
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
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new NamespacePropertyUpdate(
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
  void authorizationOutageUsesServiceUnavailableResponse() throws Exception {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(new AuthorizationServiceException("private upstream failure"));
    mvc.perform(get("/v1/catalog_name/namespaces/foo"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.error.code").value(503))
        .andExpect(jsonPath("$.error.type").value("ServiceUnavailableException"))
        .andExpect(jsonPath("$.error.message").value("Namespace metadata service unavailable"));
  }

  @Test
  void readsAllowMetadataPrivilegeButDropRequiresSystemAdmin() throws Exception {
    when(authorizationHandler.checkAccessDecision(
            anyString(), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Collections.emptyMap()));
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

  @Test
  void invalidPathReportsNamespace() throws Exception {
    mvc.perform(get("/v1/foo/namespaces/foo.bar"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.message").value(containsString("'foo.bar'")));
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
            new NamespaceMetadata("identifier", "foo", Collections.singletonMap("😀", "value")));
    mvc.perform(
            post("/v1/foo/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"],\"properties\":{\"\\ud83d\\ude00\":\"value\"}}"))
        .andExpect(status().isOk());
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new NamespacePropertyUpdate(
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
        .thenReturn(new NamespaceMetadata("private", "foo", Collections.emptyMap()));
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
    when(repository.updateProperties("foo", updates, removals))
        .thenReturn(
            new NamespacePropertyUpdate(
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

  @Test
  void conflictsPreserveMeaningForEveryAcceptedRoute() throws Exception {
    when(repository.create("foo", Map.of()))
        .thenThrow(new AlreadyExistsException("Namespace already exists"));
    mvc.perform(
            post("/v1/foo/namespaces/")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespace\":[\"foo\"]}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("AlreadyExistsException"));
    when(repository.updateProperties("namespaces", Map.of(), Set.of()))
        .thenThrow(new VersionConflictException("Namespace was modified concurrently"));
    mvc.perform(
            post("/v1/foo/namespaces/namespaces/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
    when(repository.getByName("namespaces"))
        .thenReturn(new NamespaceMetadata("identifier", "namespaces", Map.of()));
    doThrow(new VersionConflictException("Namespace was modified concurrently"))
        .when(repository)
        .delete("namespaces");
    mvc.perform(delete("/v1/foo/namespaces/namespaces"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.error.type").value("CommitFailedException"));
  }

  @Test
  void uncertainMutationUsesReconciliationErrorRatherThanUnavailable() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenThrow(
            new MutationUncertainException(
                "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."));
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
  }

  @Test
  void upstreamStatusesRemainDistinctAndMessagesStayPrivate() throws Exception {
    when(repository.getByName("foo")).thenThrow(new NotFoundException("Namespace does not exist"));
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.type").value("NoSuchNamespaceException"));
    doThrow(
            new UnavailableException(
                "Namespace metadata service unavailable", new IllegalStateException("secret")))
        .when(repository)
        .getByName("foo");
    mvc.perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.error.code").value(503))
        .andExpect(content().string(not(containsString("secret"))));
    when(repository.create(anyString(), any()))
        .thenThrow(new AlreadyExistsException("Namespace already exists"));
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
  }

  @Test
  void requiredListingFailureIsSanitizedAndPreventsDeletion() throws Exception {
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of()));
    when(repository.containsEntities("foo"))
        .thenThrow(
            new ServiceFailureException(
                "secret property value", new IllegalStateException("SQL secret")));
    mvc.perform(delete("/v1/foo/namespaces/foo"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.error.code").value(500))
        .andExpect(jsonPath("$.error.type").value("ServiceFailureException"))
        .andExpect(content().string(not(containsString("secret"))));
    verify(repository, never()).delete(anyString());
  }

  @Test
  void nonemptyDropUsesBadRequestAndPreservesMetadata() throws Exception {
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of()));
    when(repository.containsEntities("foo")).thenReturn(true);
    mvc.perform(delete("/v1/foo/namespaces/foo"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value(400))
        .andExpect(jsonPath("$.error.type").value("NamespaceNotEmptyException"));
    verify(repository, never()).delete(anyString());
  }

  @Test
  void unknownRouteDoesNotClaimNamespaceAbsence() throws Exception {
    Filter missingRoute =
        (request, response, chain) -> ((HttpServletResponse) response).sendError(404, "secret");
    MockMvc filtered =
        MockMvcBuilders.standaloneSetup(controller)
            .addFilters(new NamespaceErrorFilter(new ObjectMapper()), missingRoute)
            .build();
    filtered
        .perform(get("/v1/foo/namespaces/foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.type").value("RESTException"))
        .andExpect(jsonPath("$.error.code").value(404))
        .andExpect(content().string(not(containsString("secret"))));
  }
}
