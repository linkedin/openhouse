package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceUnavailableException;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.InvalidNamespaceException;
import com.linkedin.openhouse.tables.exception.InvalidNamespacePropertiesException;
import com.linkedin.openhouse.tables.exception.NamespaceAccessDeniedException;
import com.linkedin.openhouse.tables.exception.NamespaceAuthorizationUnavailableException;
import com.linkedin.openhouse.tables.exception.NamespaceNotEmptyException;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
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
    service.openHouseInternalRepository = mock(OpenHouseInternalRepository.class);
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
  }

  @Test
  void mutationDenialPrecedesMetadataAccess() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertThrows(
        NamespaceAccessDeniedException.class,
        () ->
            service.createNamespace(
                Collections.singletonList("foo"), Collections.emptyMap(), "bar"));
    assertThrows(
        NamespaceAccessDeniedException.class,
        () ->
            service.updateNamespaceProperties(
                "foo", Collections.emptyMap(), Collections.emptySet(), "bar"));
    assertThrows(NamespaceAccessDeniedException.class, () -> service.dropNamespace("foo", "bar"));
  }

  @Test
  void readDenialPrecedesMetadataLookupAndHidesExistence() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertThrows(
        NamespaceAccessDeniedException.class, () -> service.loadNamespaceMetadata("foo", "bar"));
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"),
            argThat((DatabaseDto database) -> database.getDatabaseId().equals("foo")),
            eq(Privileges.GET_TABLE_METADATA));
  }

  @Test
  void createAndUpdateRequireSystemAdminAndForwardSparseChanges() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenReturn(new NamespaceMetadata("identifier", "foo", Collections.singletonMap("a", "1")));
    service.createNamespace(
        Collections.singletonList("foo"), Collections.singletonMap("a", "1"), "bar");
    verify(authorizationHandler)
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new NamespacePropertyUpdate(
                Collections.singletonList("b"),
                Collections.singletonList("a"),
                Collections.singletonList("c")));
    assertEquals(
        Collections.singletonList("c"),
        service
            .updateNamespaceProperties(
                "foo", Collections.singletonMap("b", "2"), Collections.singleton("a"), "bar")
            .getMissing());
    verify(repository)
        .updateProperties("foo", Collections.singletonMap("b", "2"), Collections.singleton("a"));
    verify(authorizationHandler, times(2))
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
  }

  @Test
  void overlappingPropertyChangesAreRejectedBeforeMutation() throws Exception {
    assertThrows(
        InvalidNamespacePropertiesException.class,
        () ->
            service.updateNamespaceProperties(
                "foo", Collections.singletonMap("a", "1"), Collections.singleton("a"), "bar"));
    verify(repository, never()).updateProperties(anyString(), anyMap(), anySet());
  }

  @Test
  void namespaceHierarchyIsRejectedBeforeMetadataLookup() throws Exception {
    assertThrows(
        InvalidNamespaceException.class,
        () -> service.loadNamespaceMetadata("foo\u001fbar", "bar"));
    verify(repository, never()).findByName(anyString());
  }

  @Test
  void missingNamespaceDropStopsBeforeOccupancy() throws Exception {
    when(repository.findByName("foo")).thenReturn(Optional.empty());
    assertThrows(HouseNamespaceNotFoundException.class, () -> service.dropNamespace("foo", "bar"));
    verify(repository, never()).containsEntities(anyString());
    verify(repository, never()).delete(anyString());
  }

  @Test
  void occupiedNamespaceDropPreservesMetadata() throws Exception {
    NamespaceMetadata metadata =
        new NamespaceMetadata("identifier", "foo", Collections.singletonMap("a", "1"));
    AtomicReference<Optional<NamespaceMetadata>> stored =
        new AtomicReference<>(Optional.of(metadata));
    when(repository.findByName("foo")).thenAnswer(invocation -> stored.get());
    doAnswer(
            invocation -> {
              stored.set(Optional.empty());
              return null;
            })
        .when(repository)
        .delete("foo");
    when(repository.containsEntities("foo")).thenReturn(true);
    assertThrows(NamespaceNotEmptyException.class, () -> service.dropNamespace("foo", "bar"));
    assertEquals(Optional.of(metadata), stored.get());
  }

  @Test
  void emptyNamespaceDropDeletesMetadataAfterAdministrativeAndOccupancyChecks() throws Exception {
    AtomicReference<Optional<NamespaceMetadata>> stored =
        new AtomicReference<>(
            Optional.of(new NamespaceMetadata("identifier", "foo", Collections.emptyMap())));
    when(repository.findByName("foo")).thenAnswer(invocation -> stored.get());
    doAnswer(
            invocation -> {
              stored.set(Optional.empty());
              return null;
            })
        .when(repository)
        .delete("foo");
    when(repository.containsEntities("foo")).thenReturn(false);
    service.dropNamespace("foo", "bar");
    assertEquals(Optional.empty(), stored.get());
    InOrder checks = inOrder(authorizationHandler, repository);
    checks
        .verify(authorizationHandler)
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.SYSTEM_ADMIN));
    checks.verify(repository).findByName("foo");
    checks.verify(repository).containsEntities("foo");
    checks.verify(repository).delete("foo");
  }

  @Test
  void metadataOutagePreventsDropAndOccupancyLookup() throws Exception {
    when(repository.findByName("foo")).thenThrow(new HouseNamespaceUnavailableException());
    assertThrows(
        HouseNamespaceUnavailableException.class, () -> service.dropNamespace("foo", "bar"));
    verify(repository, never()).containsEntities(anyString());
    verify(repository, never()).delete(anyString());
  }

  @Test
  void authorizationOutagePreventsMetadataLoadAndPreservesCause() throws Exception {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(new AuthorizationServiceException("secret"));
    NamespaceAuthorizationUnavailableException exception =
        assertThrows(
            NamespaceAuthorizationUnavailableException.class,
            () -> service.loadNamespaceMetadata("foo", "bar"));
    assertFalse(exception.getMessage().contains("secret"));
    assertInstanceOf(AuthorizationServiceException.class, exception.getCause());
    verify(repository, never()).findByName(anyString());
  }

  @Test
  void failedOccupancyPreventsDeletion() throws Exception {
    when(repository.findByName("foo"))
        .thenReturn(
            Optional.of(new NamespaceMetadata("identifier", "foo", Collections.emptyMap())));
    when(repository.containsEntities("foo")).thenThrow(new HouseNamespaceUnavailableException());
    assertThrows(
        HouseNamespaceUnavailableException.class, () -> service.dropNamespace("foo", "bar"));
    verify(repository, never()).delete(anyString());
  }

  @Test
  void mixedCaseAuthorizationUsesNormalizedIdentity() throws Exception {
    when(repository.findByName("FOO"))
        .thenReturn(
            Optional.of(new NamespaceMetadata("identifier", "foo", Collections.emptyMap())));
    service.loadNamespaceMetadata("FOO", "bar");
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"),
            argThat((DatabaseDto database) -> database.getDatabaseId().equals("foo")),
            eq(Privileges.GET_TABLE_METADATA));
    reset(repository);
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    NamespaceAccessDeniedException denied =
        assertThrows(
            NamespaceAccessDeniedException.class,
            () -> service.loadNamespaceMetadata("FoO", "bar"));
    assertInstanceOf(AccessDeniedException.class, denied.getCause());
  }
}
