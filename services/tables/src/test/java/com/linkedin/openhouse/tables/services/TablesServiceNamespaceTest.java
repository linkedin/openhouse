package com.linkedin.openhouse.tables.services;

import static com.linkedin.openhouse.tables.exception.NamespaceException.Reason.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException.Reason;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
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
    service.openHouseInternalRepository = mock(OpenHouseInternalRepository.class);
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
  }

  @Test
  void mutationDenialPrecedesMetadataAccess() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertEquals(
        ACCESS_DENIED,
        assertThrows(
                NamespaceException.class,
                () ->
                    service.createNamespace(
                        Collections.singletonList("foo"), Collections.emptyMap(), "bar"))
            .getReason());
    assertEquals(
        ACCESS_DENIED,
        assertThrows(
                NamespaceException.class,
                () ->
                    service.updateNamespaceProperties(
                        "foo", Collections.emptyMap(), Collections.emptySet(), "bar"))
            .getReason());
    assertEquals(
        ACCESS_DENIED,
        assertThrows(NamespaceException.class, () -> service.dropNamespace("foo", "bar"))
            .getReason());
    verifyNoInteractions(repository);
  }

  @AfterEach
  void namespaceOperationsUseIndependentRepository() {
    verifyNoInteractions(service.openHouseInternalRepository);
  }

  @Test
  void readDenialPrecedesMetadataLookupAndHidesExistence() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertEquals(
        ACCESS_DENIED,
        assertThrows(NamespaceException.class, () -> service.loadNamespaceMetadata("foo", "bar"))
            .getReason());
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"),
            argThat((DatabaseDto database) -> database.getDatabaseId().equals("foo")),
            eq(Privileges.GET_TABLE_METADATA));
    verifyNoInteractions(repository);
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
  void overlapAndHierarchyAreRejectedBeforeMutation() {
    assertEquals(
        INVALID_PROPERTIES,
        assertThrows(
                NamespaceException.class,
                () ->
                    service.updateNamespaceProperties(
                        "foo",
                        Collections.singletonMap("a", "1"),
                        Collections.singleton("a"),
                        "bar"))
            .getReason());
    assertEquals(
        INVALID_NAMESPACE,
        assertThrows(
                NamespaceException.class,
                () -> service.loadNamespaceMetadata("foo\u001fbar", "bar"))
            .getReason());
    verifyNoInteractions(repository);
  }

  @Test
  void dropChecksMetadataBeforeOccupancyAndNeverDeletesOccupiedNamespaces() throws Exception {
    when(repository.findByName("foo")).thenReturn(Optional.empty());
    assertEquals(
        Reason.NOT_FOUND,
        assertThrows(HouseNamespaceException.class, () -> service.dropNamespace("foo", "bar"))
            .getReason());
    verify(repository, never()).containsEntities(anyString());
    when(repository.findByName("foo"))
        .thenReturn(
            Optional.of(new NamespaceMetadata("identifier", "foo", Collections.emptyMap())));
    when(repository.containsEntities("foo")).thenReturn(true);
    assertEquals(
        NOT_EMPTY,
        assertThrows(NamespaceException.class, () -> service.dropNamespace("foo", "bar"))
            .getReason());
    verify(repository, never()).delete(anyString());
    when(repository.containsEntities("foo")).thenReturn(false);
    service.dropNamespace("foo", "bar");
    verify(repository).delete("foo");
    verify(authorizationHandler, times(3))
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.DELETE_TABLE));
  }

  @Test
  void outageRemainsVisibleAndStopsDrop() throws Exception {
    when(repository.findByName("foo")).thenThrow(new HouseNamespaceException(Reason.UNAVAILABLE));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseNamespaceException.class, () -> service.dropNamespace("foo", "bar"))
            .getReason());
    verify(repository, never()).containsEntities(anyString());
    verify(repository, never()).delete(anyString());
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(new AuthorizationServiceException("secret"));
    NamespaceException exception =
        assertThrows(NamespaceException.class, () -> service.loadNamespaceMetadata("foo", "bar"));
    assertEquals(AUTHORIZATION_UNAVAILABLE, exception.getReason());
    assertFalse(exception.getMessage().contains("secret"));
    assertInstanceOf(AuthorizationServiceException.class, exception.getCause());
  }

  @Test
  void failedOccupancyPreventsDeletion() throws Exception {
    when(repository.findByName("foo"))
        .thenReturn(
            Optional.of(new NamespaceMetadata("identifier", "foo", Collections.emptyMap())));
    when(repository.containsEntities("foo"))
        .thenThrow(new HouseNamespaceException(Reason.UNAVAILABLE));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseNamespaceException.class, () -> service.dropNamespace("foo", "bar"))
            .getReason());
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
    NamespaceException denied =
        assertThrows(NamespaceException.class, () -> service.loadNamespaceMetadata("FoO", "bar"));
    assertEquals(ACCESS_DENIED, denied.getReason());
    assertInstanceOf(AccessDeniedException.class, denied.getCause());
    verifyNoInteractions(repository);
  }
}
