package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.InvalidPropertyUpdateException;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.test.util.ReflectionTestUtils;

class TablesServiceNamespaceTest {
  private final HouseNamespaceRepository repository = mock(HouseNamespaceRepository.class);
  private final AuthorizationHandler authorizationHandler = mock(AuthorizationHandler.class);
  private TablesServiceImpl service;

  @BeforeEach
  void setUp() {
    AuthorizationUtils authorizationUtils = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorizationUtils, "authorizationHandler", authorizationHandler);
    service = new TablesServiceImpl();
    service.namespaceRepository = repository;
    service.authorizationUtils = authorizationUtils;
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
  }

  @Test
  void deniedNamespaceOperationsUseAccessDeniedException() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertThrows(
        AccessDeniedException.class,
        () -> service.createNamespace("foo", Collections.emptyMap(), "bar"));
    assertThrows(
        AccessDeniedException.class,
        () ->
            service.updateNamespaceProperties(
                "foo", Collections.emptyMap(), Collections.emptySet(), "bar"));
    assertThrows(AccessDeniedException.class, () -> service.dropNamespace("foo", "bar"));
    verifyNoInteractions(repository);
  }

  @Test
  void createAndUpdateRequireSystemAdminAndReplaceDocument() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenReturn(
            new NamespaceMetadata("identifier", "foo", Collections.singletonMap("a", "1"), "1"));
    service.createNamespace("foo", Collections.singletonMap("a", "1"), "bar");
    verify(authorizationHandler)
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
    when(repository.getByName("foo"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Map.of("a", "1", "keep", "x"), "1"));
    assertEquals(
        Collections.singletonList("c"),
        service
            .updateNamespaceProperties(
                "foo", Collections.singletonMap("b", "2"), Set.of("a", "c"), "bar")
            .getMissing());
    verify(repository)
        .update(
            "foo", new NamespaceMetadata("identifier", "foo", Map.of("b", "2", "keep", "x"), "1"));
    verify(authorizationHandler, times(2))
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
  }

  @Test
  void overlappingPropertyChangesAreUnprocessable() {
    assertThrows(
        InvalidPropertyUpdateException.class,
        () ->
            service.updateNamespaceProperties(
                "foo", Collections.singletonMap("a", "1"), Collections.singleton("a"), "bar"));
    verifyNoInteractions(repository);
  }

  @Test
  void updateConflictIsNotRetried() throws Exception {
    NamespaceMetadata current =
        new NamespaceMetadata("identifier", "foo", Map.of("keep", "1"), "1");
    when(repository.getByName("foo")).thenReturn(current);
    EntityConcurrentModificationException conflict =
        new EntityConcurrentModificationException("foo", null);
    when(repository.update(eq("foo"), any())).thenThrow(conflict);
    assertSame(
        conflict,
        assertThrows(
            EntityConcurrentModificationException.class,
            () -> service.updateNamespaceProperties("foo", Map.of("a", "2"), Set.of(), "bar")));
    verify(repository, times(1)).getByName("foo");
    verify(repository, times(1)).update(eq("foo"), any());
  }

  @Test
  void dropAuthorizesAndDeletesNamespaceMetadata() throws Exception {
    service.dropNamespace("foo", "bar");
    verify(authorizationHandler)
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
    verify(repository).delete("foo");
  }

  @Test
  void authorizationOutagePreservesCause() {
    AuthorizationServiceException failure = new AuthorizationServiceException("secret");
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(failure);
    assertSame(
        failure,
        assertThrows(
            AuthorizationServiceException.class,
            () -> service.loadNamespaceMetadata("foo", "bar")));
  }

  @Test
  void mixedCaseAuthorizationUsesNormalizedIdentity() throws Exception {
    when(repository.getByName("FOO"))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Collections.emptyMap(), "1"));
    service.loadNamespaceMetadata("FOO", "bar");
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"),
            argThat((DatabaseDto database) -> database.getDatabaseId().equals("foo")),
            eq(Privileges.GET_TABLE_METADATA));
  }
}
