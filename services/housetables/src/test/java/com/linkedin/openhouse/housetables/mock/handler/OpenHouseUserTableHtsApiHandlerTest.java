package com.linkedin.openhouse.housetables.mock.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.handler.OpenHouseUserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * The handler is where transport stops. The service is mocked, so nothing here restates its
 * behaviour.
 */
public class OpenHouseUserTableHtsApiHandlerTest {

  private static final String DB = "handler_db";

  private HouseTablesApiValidator<UserTableKey, UserTable> validator;

  private UserTablesService userTablesService;

  private OpenHouseUserTableHtsApiHandler handler;

  private final UserTablesMapper userTablesMapper =
      new com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapperImpl();

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setup() {
    validator = Mockito.mock(HouseTablesApiValidator.class);
    userTablesService = Mockito.mock(UserTablesService.class);

    handler = new OpenHouseUserTableHtsApiHandler();
    ReflectionTestUtils.setField(handler, "userTablesHtsApiValidator", validator);
    ReflectionTestUtils.setField(handler, "userTableService", userTablesService);
    ReflectionTestUtils.setField(handler, "userTablesMapper", userTablesMapper);
  }

  private static UserTableKey key(String tableId) {
    return UserTableKey.builder().databaseId(DB).tableId(tableId).build();
  }

  private static Page<UserTableDto> emptyViewPage() {
    return new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 50), 0);
  }

  private static UserTableDto dto(String tableId, EntityType entityType) {
    return UserTableDto.builder()
        .databaseId(DB)
        .tableId(tableId)
        .metadataLocation(String.format("/openhouse/%s/%s/v0_metadata.json", DB, tableId))
        .tableVersion(String.format("/openhouse/%s/%s/v0_metadata.json", DB, tableId))
        .entityType(entityType)
        .build();
  }

  // -------------------------------------------------------------------------------------------
  // validation ordering
  // -------------------------------------------------------------------------------------------

  /** Constructing a query value first would answer 500 for an invalid filter instead of 400. */
  @Test
  public void testViewQueryIsValidatedBeforeTheServiceIsReached() {
    doThrow(new RequestValidationFailureException("Only databaseId and tableId are supported"))
        .when(validator)
        .validateGetEntities(any(UserTable.class), anyInt(), anyInt(), any());

    assertThatThrownBy(
            () ->
                handler.getViewEntities(
                    UserTable.builder().creationTime(123L).build(), 0, 50, null))
        .isInstanceOf(RequestValidationFailureException.class);

    Mockito.verify(userTablesService, Mockito.never())
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
  }

  @Test
  public void testPagedViewQueryIsValidatedIncludingItsPaging() {
    doThrow(new RequestValidationFailureException("page cannot be negative"))
        .when(validator)
        .validateGetEntities(any(UserTable.class), anyInt(), anyInt(), any());

    assertThatThrownBy(
            () -> handler.getViewEntities(UserTable.builder().databaseId(DB).build(), -1, 50, null))
        .isInstanceOf(RequestValidationFailureException.class);

    Mockito.verify(userTablesService, Mockito.never())
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
  }

  @Test
  public void testViewPointReadAndDeleteAreValidatedFirst() {
    doThrow(new RequestValidationFailureException("bad key"))
        .when(validator)
        .validateGetEntity(any(UserTableKey.class));
    doThrow(new RequestValidationFailureException("bad key"))
        .when(validator)
        .validateDeleteEntity(any(UserTableKey.class));

    assertThatThrownBy(() -> handler.getViewEntity(key("bad??id")))
        .isInstanceOf(RequestValidationFailureException.class);
    assertThatThrownBy(() -> handler.getNeutralEntity(key("bad??id")))
        .isInstanceOf(RequestValidationFailureException.class);
    assertThatThrownBy(() -> handler.deleteView(key("bad??id")))
        .isInstanceOf(RequestValidationFailureException.class);

    Mockito.verifyNoInteractions(userTablesService);
  }

  /**
   * A {@code tableId} with no {@code databaseId} is the one shape the owned query type refuses to
   * construct, so building before validating would answer 500 instead of the validator's 400.
   */
  @Test
  public void testValidationHappensBeforeQueryConstruction() {
    doThrow(new RequestValidationFailureException("tableId cannot be provided without databaseId"))
        .when(validator)
        .validateGetEntities(any(UserTable.class), anyInt(), anyInt(), any());

    assertThatThrownBy(
            () -> handler.getViewEntities(UserTable.builder().tableId("t0%").build(), 0, 50, null))
        .isInstanceOf(RequestValidationFailureException.class)
        .hasMessageContaining("tableId cannot be provided without databaseId");

    Mockito.verify(userTablesService, Mockito.never())
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
  }

  /** And the ordinary case still calls validate, then build, then the service, in that order. */
  @Test
  public void testAcceptedRequestValidatesThenCallsTheService() {
    doReturn(emptyViewPage())
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());

    handler.getViewEntities(UserTable.builder().databaseId(DB).build(), 0, 50, null);

    InOrder inOrder = Mockito.inOrder(validator, userTablesService);
    inOrder.verify(validator).validateGetEntities(any(UserTable.class), anyInt(), anyInt(), any());
    inOrder
        .verify(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
  }

  // -------------------------------------------------------------------------------------------
  // transport does not cross into the service
  // -------------------------------------------------------------------------------------------

  /** An empty filter map is the unbounded view query, not a database-name projection. */
  @Test
  public void testEachQueryShapeMapsToItsOwnedQueryValue() {
    doReturn(emptyViewPage())
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
    ArgumentCaptor<UserViewQuery> captor = ArgumentCaptor.forClass(UserViewQuery.class);

    handler.getViewEntities(UserTable.builder().build(), 0, 50, null);
    handler.getViewEntities(UserTable.builder().databaseId(DB).build(), 0, 50, null);
    handler.getViewEntities(UserTable.builder().databaseId(DB).tableId("t0%").build(), 0, 50, null);

    Mockito.verify(userTablesService, Mockito.times(3))
        .getAllUserViews(captor.capture(), anyInt(), anyInt(), any());
    List<UserViewQuery> queries = captor.getAllValues();

    assertThat(queries.get(0).getDatabaseId()).isEmpty();
    assertThat(queries.get(0).getTableIdPattern()).isEmpty();

    assertThat(queries.get(1).getDatabaseId()).hasValue(DB);
    assertThat(queries.get(1).getTableIdPattern()).isEmpty();

    assertThat(queries.get(2).getDatabaseId()).hasValue(DB);
    assertThat(queries.get(2).getTableIdPattern()).hasValue("t0%");
  }

  /** The paged route drops it at the same boundary; the two routes must not diverge here. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"TABLE", "table", "VIEW", "ViEw", "UNKNOWN"})
  public void testEntityTypeNeverCrossesIntoTheOwnedPagedQuery(String entityType) {
    doReturn(new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 0))
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
    ArgumentCaptor<UserViewQuery> captor = ArgumentCaptor.forClass(UserViewQuery.class);

    handler.getViewEntities(
        UserTable.builder().databaseId(DB).tableId("t0%").entityType(entityType).build(),
        0,
        2,
        "tableId");

    // The paging arguments are matched in the verify; the query is captured so the equality
    // failure names the field that leaked.
    Mockito.verify(userTablesService)
        .getAllUserViews(captor.capture(), eq(0), eq(2), eq("tableId"));
    Assertions.assertEquals(UserViewQuery.matchingPattern(DB, "t0%"), captor.getValue());
  }

  @Test
  public void testPagedQueryCarriesItsPagingIntoTheOwnedValue() {
    doReturn(new PageImpl<>(Collections.emptyList(), PageRequest.of(1, 2), 0))
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
    ArgumentCaptor<UserViewQuery> captor = ArgumentCaptor.forClass(UserViewQuery.class);

    handler.getViewEntities(UserTable.builder().databaseId(DB).build(), 1, 2, "tableId");

    // eq() on each paging argument fails the verify on any mismatch, so page, size and sortBy are
    // pinned exactly as they were when they travelled inside one object.
    Mockito.verify(userTablesService)
        .getAllUserViews(captor.capture(), eq(1), eq(2), eq("tableId"));
    assertThat(captor.getValue().getDatabaseId()).hasValue(DB);
  }

  @Test
  public void testOmittedSortStaysAbsentAtTheServiceBoundary() {
    doReturn(new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 50), 0))
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());
    handler.getViewEntities(UserTable.builder().databaseId(DB).build(), 0, 50, null);

    // Absent stays absent: the handler substitutes no default, leaving the service to apply
    // "tableId". isNull() fails if anything was filled in on the way.
    Mockito.verify(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), eq(0), eq(50), isNull());
  }

  // -------------------------------------------------------------------------------------------
  // absence becomes not-found, here and only here
  // -------------------------------------------------------------------------------------------

  /** The neutral read names the entity generically, because either type may occupy the key. */
  @Test
  public void testAbsentNeutralEntityBecomesTheEntityNotFoundMessage() {
    doReturn(Optional.empty()).when(userTablesService).getNeutralEntity(DB, "absent");

    assertThatThrownBy(() -> handler.getNeutralEntity(key("absent")))
        .isInstanceOf(NoSuchEntityException.class)
        .hasMessage("Entity " + DB + ".absent cannot be found");
  }

  @Test
  public void testAbsentViewBecomesTheViewNotFoundMessage() {
    doReturn(Optional.empty()).when(userTablesService).getUserView(DB, "absent");

    assertThatThrownBy(() -> handler.getViewEntity(key("absent")))
        .isInstanceOf(NoSuchEntityException.class)
        .hasMessage("View " + DB + ".absent cannot be found");
  }

  /**
   * A view delete that removed nothing is the same not-found, and it is the handler that says so.
   */
  @Test
  public void testUnsuccessfulViewDeleteBecomesTheViewNotFoundMessage() {
    doReturn(false).when(userTablesService).deleteUserView(DB, "absent");

    assertThatThrownBy(() -> handler.deleteView(key("absent")))
        .isInstanceOf(NoSuchEntityException.class)
        .hasMessage("View " + DB + ".absent cannot be found");
  }

  @Test
  public void testSuccessfulViewDeleteIsNoContentWithNoBody() {
    doReturn(true).when(userTablesService).deleteUserView(DB, "drop_view");

    ApiResponse<Void> response = handler.deleteView(key("drop_view"));

    Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getHttpStatus());
    Assertions.assertNull(response.getResponseBody());
  }

  @Test
  public void testPersistenceFailureIsNotConvertedIntoNotFound() {
    doThrow(new com.linkedin.openhouse.common.exception.CorruptEntityTypeException("corrupt row"))
        .when(userTablesService)
        .getNeutralEntity(anyString(), anyString());

    assertThatThrownBy(() -> handler.getNeutralEntity(key("corrupt")))
        .isInstanceOf(com.linkedin.openhouse.common.exception.CorruptEntityTypeException.class);
  }

  // -------------------------------------------------------------------------------------------
  // response mapping
  // -------------------------------------------------------------------------------------------

  @Test
  public void testPointReadsAnswerOkWithTheCanonicalWireType() {
    doReturn(Optional.of(dto("a_view", EntityType.VIEW)))
        .when(userTablesService)
        .getUserView(DB, "a_view");
    doReturn(Optional.of(dto("legacy", EntityType.TABLE)))
        .when(userTablesService)
        .getNeutralEntity(DB, "legacy");

    ApiResponse<EntityResponseBody<UserTable>> view = handler.getViewEntity(key("a_view"));
    Assertions.assertEquals(HttpStatus.OK, view.getHttpStatus());
    Assertions.assertEquals("VIEW", view.getResponseBody().getEntity().getEntityType());

    ApiResponse<EntityResponseBody<UserTable>> neutral = handler.getNeutralEntity(key("legacy"));
    Assertions.assertEquals(HttpStatus.OK, neutral.getHttpStatus());
    Assertions.assertEquals("TABLE", neutral.getResponseBody().getEntity().getEntityType());
  }

  @Test
  public void testPagedViewQueryAnswersWithPageResultsAndKeepsPagingMetadata() {
    doReturn(
            new PageImpl<>(
                Collections.singletonList(dto("v1", EntityType.VIEW)), PageRequest.of(0, 2), 3))
        .when(userTablesService)
        .getAllUserViews(any(UserViewQuery.class), anyInt(), anyInt(), any());

    ApiResponse<GetAllEntityResponseBody<UserTable>> response =
        handler.getViewEntities(UserTable.builder().databaseId(DB).build(), 0, 2, "tableId");

    Page<UserTable> pageResults = pageResults(response.getResponseBody());
    Assertions.assertEquals(3, pageResults.getTotalElements());
    Assertions.assertEquals(2, pageResults.getTotalPages());
    Assertions.assertEquals("VIEW", pageResults.getContent().get(0).getEntityType());
    // The unpaged slot stays empty, so the two bodies remain distinguishable on the wire.
    assertThat(results(response.getResponseBody())).isNull();
  }

  /** {@link GetAllEntityResponseBody} exposes no accessors, so the fields are read directly. */
  @SuppressWarnings("unchecked")
  private static List<UserTable> results(GetAllEntityResponseBody<UserTable> body) {
    return (List<UserTable>) ReflectionTestUtils.getField(body, "results");
  }

  @SuppressWarnings("unchecked")
  private static Page<UserTable> pageResults(GetAllEntityResponseBody<UserTable> body) {
    return (Page<UserTable>) ReflectionTestUtils.getField(body, "pageResults");
  }

  // -------------------------------------------------------------------------------------------
  // create versus update
  // -------------------------------------------------------------------------------------------

  /**
   * A first write is 201 and an overwrite is 200, on the view route exactly as on the table one.
   */
  @Test
  public void testViewPutSelects201OnCreateAnd200OnUpdate() {
    UserTable submitted =
        UserTable.builder()
            .databaseId(DB)
            .tableId("put_view")
            .tableVersion("INITIAL_VERSION")
            .metadataLocation("/openhouse/handler_db/put_view/v0_metadata.json")
            .entityType("VIEW")
            .build();

    doReturn(Pair.of(dto("put_view", EntityType.VIEW), false))
        .when(userTablesService)
        .putUserView(submitted);
    ApiResponse<EntityResponseBody<UserTable>> created = handler.putView(submitted);
    Assertions.assertEquals(HttpStatus.CREATED, created.getHttpStatus());
    Assertions.assertEquals("VIEW", created.getResponseBody().getEntity().getEntityType());

    doReturn(Pair.of(dto("put_view", EntityType.VIEW), true))
        .when(userTablesService)
        .putUserView(submitted);
    Assertions.assertEquals(HttpStatus.OK, handler.putView(submitted).getHttpStatus());
  }

  @Test
  public void testViewPutCallsTheViewWriteAndTableWriteIsUntouched() {
    UserTable submitted =
        UserTable.builder()
            .databaseId(DB)
            .tableId("put_view")
            .tableVersion("INITIAL_VERSION")
            .metadataLocation("/openhouse/handler_db/put_view/v0_metadata.json")
            .entityType("VIEW")
            .build();
    doReturn(Pair.of(dto("put_view", EntityType.VIEW), false))
        .when(userTablesService)
        .putUserView(any(UserTable.class));

    handler.putView(submitted);

    Mockito.verify(userTablesService).putUserView(submitted);
    Mockito.verify(userTablesService, Mockito.never()).putUserTable(any(UserTable.class));
  }

  /** Regression: the table route still calls the table write and is validated the same way. */
  @Test
  public void testTablePutStillCallsTheTableWrite() {
    UserTable submitted =
        UserTable.builder()
            .databaseId(DB)
            .tableId("put_table")
            .tableVersion("INITIAL_VERSION")
            .metadataLocation("/openhouse/handler_db/put_table/v0_metadata.json")
            .entityType("TABLE")
            .build();
    doReturn(Pair.of(dto("put_table", EntityType.TABLE), false))
        .when(userTablesService)
        .putUserTable(any(UserTable.class));

    Assertions.assertEquals(HttpStatus.CREATED, handler.putEntity(submitted).getHttpStatus());

    Mockito.verify(validator).validatePutEntity(submitted);
    Mockito.verify(userTablesService).putUserTable(submitted);
    Mockito.verify(userTablesService, Mockito.never()).putUserView(any(UserTable.class));
  }
}
