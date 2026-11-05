package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.housetables.client.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.client.model.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import com.linkedin.openhouse.tables.api.spec.namespace.CreateNamespaceRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesRequest;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.test.util.ReflectionTestUtils;

class IcebergNamespacesServiceTest {
  private final HouseDatabaseRepository repository = mock(HouseDatabaseRepository.class);
  private final AuthorizationHandler authorizationHandler = mock(AuthorizationHandler.class);
  private IcebergNamespacesService service;

  @BeforeEach
  void setUp() {
    AuthorizationUtils authorizationUtils = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorizationUtils, "authorizationHandler", authorizationHandler);
    service = new IcebergNamespacesService(repository, authorizationUtils);
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(true);
  }

  @Test
  void readDenialPrecedesMetadataLookupAndHidesExistence() {
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenReturn(false);
    assertEquals(
        HttpStatus.FORBIDDEN,
        assertThrows(NamespaceException.class, () -> service.load("foo", "bar")).getStatus());
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"),
            argThat((DatabaseDto database) -> database.getDatabaseId().equals("foo")),
            eq(Privileges.GET_TABLE_METADATA));
    verifyNoInteractions(repository);
  }

  @Test
  void createAndUpdateUseDatabasePrivilegesAndForwardSparseChanges() throws Exception {
    when(repository.create(eq("foo"), any()))
        .thenReturn(
            new DatabaseMetadata()
                .databaseName("foo")
                .properties(Collections.singletonMap("a", "1")));
    service.create(
        CreateNamespaceRequest.builder()
            .namespace(Collections.singletonList("foo"))
            .properties(Collections.singletonMap("a", "1"))
            .build(),
        "bar");
    verify(authorizationHandler)
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.CREATE_TABLE));
    when(repository.updateProperties(eq("foo"), any(), any()))
        .thenReturn(
            new UpdateDatabasePropertiesResponseBody()
                .updated(Collections.singletonList("b"))
                .removed(Collections.singletonList("a"))
                .missing(Collections.singletonList("c")));
    assertEquals(
        Collections.singletonList("c"),
        service
            .update(
                "foo",
                UpdateNamespacePropertiesRequest.builder()
                    .updates(Collections.singletonMap("b", "2"))
                    .removals(Collections.singleton("a"))
                    .build(),
                "bar")
            .getMissing());
    verify(repository)
        .updateProperties("foo", Collections.singletonMap("b", "2"), Collections.singleton("a"));
    verify(authorizationHandler)
        .checkAccessDecision(
            eq("bar"), any(DatabaseDto.class), eq(Privileges.UPDATE_TABLE_METADATA));
  }

  @Test
  void overlapAndHierarchyAreRejectedBeforeMutation() {
    assertEquals(
        HttpStatus.UNPROCESSABLE_ENTITY,
        assertThrows(
                NamespaceException.class,
                () ->
                    service.update(
                        "foo",
                        UpdateNamespacePropertiesRequest.builder()
                            .updates(Collections.singletonMap("a", "1"))
                            .removals(Collections.singleton("a"))
                            .build(),
                        "bar"))
            .getStatus());
    assertEquals(
        HttpStatus.BAD_REQUEST,
        assertThrows(NamespaceException.class, () -> service.load("foo\u001fbar", "bar"))
            .getStatus());
    verifyNoInteractions(repository);
  }

  @Test
  void dropChecksMetadataBeforeOccupancyAndNeverDeletesOccupiedNamespaces() throws Exception {
    when(repository.findByName("foo")).thenReturn(Optional.empty());
    assertEquals(
        Reason.NOT_FOUND,
        assertThrows(HouseDatabaseException.class, () -> service.drop("foo", "bar")).getReason());
    verify(repository, never()).containsEntities(anyString());
    when(repository.findByName("foo")).thenReturn(Optional.of(new DatabaseMetadata()));
    when(repository.containsEntities("foo")).thenReturn(true);
    assertEquals(
        "NamespaceNotEmptyException",
        assertThrows(NamespaceException.class, () -> service.drop("foo", "bar")).getType());
    verify(repository, never()).delete(anyString());
    when(repository.containsEntities("foo")).thenReturn(false);
    service.drop("foo", "bar");
    verify(repository).delete("foo");
    verify(authorizationHandler, times(3))
        .checkAccessDecision(eq("bar"), any(DatabaseDto.class), eq(Privileges.DELETE_TABLE));
  }

  @Test
  void outageRemainsVisibleAndStopsDrop() throws Exception {
    when(repository.findByName("foo")).thenThrow(new HouseDatabaseException(Reason.UNAVAILABLE));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseDatabaseException.class, () -> service.drop("foo", "bar")).getReason());
    verify(repository, never()).containsEntities(anyString());
    verify(repository, never()).delete(anyString());
    when(authorizationHandler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenThrow(new AuthorizationServiceException("secret"));
    NamespaceException exception =
        assertThrows(NamespaceException.class, () -> service.load("foo", "bar"));
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatus());
    assertFalse(exception.getMessage().contains("secret"));
  }
}
