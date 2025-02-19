package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.linkedin.openhouse.gen.tables.client.model.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.iceberg.exceptions.ValidationException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class GrantStatementTest {

  @Test
  public void testGrantStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_VIEWER")));
    String ddlWithSchema = "GRANT SELECT ON TABLE openhouse.dgrant.t1 TO sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testGrantStatementIdentifierWithLeadingDigits() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_VIEWER")));
    String ddlWithSchema = "GRANT SELECT ON TABLE openhouse.0_.0_ TO sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testGrantStatementPublicUser() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "*", "TABLE_VIEWER")));
    String ddlWithSchema = "GRANT SELECT ON TABLE openhouse.dgrant.t1 TO PUBLIC";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testRevokeStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("REVOKE", "sraikar", "TABLE_VIEWER")));
    String ddlWithSchema = "REVOKE SELECT ON TABLE openhouse.dgrant.t1 FROM sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testRevokeStatementPublicUser() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("REVOKE", "*", "TABLE_VIEWER")));
    String ddlWithSchema = "REVOKE SELECT ON TABLE openhouse.dgrant.t1 FROM PUBLIC";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testManageGrantStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "ACL_EDITOR")));
    String ddlWithSchema = "GRANT MANAGE GRANTS ON TABLE openhouse.dgrant.t1 TO sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testDescribeStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("REVOKE", "sraikar", "TABLE_VIEWER")));
    String ddlWithSchema = "REVOKE DESCRIBE ON TABLE openhouse.dgrant.t1 FROM sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testAlterStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_ADMIN")));
    String ddlWithSchema = "GRANT ALTER ON TABLE openhouse.dgrant.t1 TO sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testGrantDbStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_CREATOR")));
    String ddlWithSchema = "GRANT CREATE TABLE ON DATABASE openhouse.db TO sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testRevokeDbStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("REVOKE", "sraikar", "TABLE_CREATOR")));
    String ddlWithSchema = "REVOKE CREATE TABLE ON DATABASE openhouse.db FROM sraikar";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testGrantMultiNamespace() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_CREATOR")));
    String ddlWithSchema = "GRANT CREATE TABLE ON DATABASE openhouse.db1.db2 TO sraikar";
    ValidationException exception =
        Assertions.assertThrows(ValidationException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(
        exception.getMessage().contains("Input namespace has more than one levels"));
  }

  @Test
  public void testShowGrantsForEmptyResponse() {
    mockTableService.enqueue(mockResponse(200, mockGetAclPoliciesResponseBody()));
    assert spark.sql("SHOW GRANTS ON TABLE openhouse.db.table").collectAsList().isEmpty();
  }

  @Test
  public void testShowGrantsForTable() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetAclPoliciesResponseBody(
                mockAclPolicy("ACL_EDITOR", "sraikar"),
                mockAclPolicy("TABLE_VIEWER", "sraikar"),
                mockAclPolicy("TABLE_VIEWER", "lesun"),
                mockAclPolicy("TABLE_ADMIN", "lejiang"))));
    List<String> actualRows =
        spark.sql("SHOW GRANTS ON TABLE openhouse.db.table").collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        actualRows.containsAll(
            ImmutableList.of(
                "MANAGE GRANTS.sraikar", "SELECT.sraikar", "SELECT.lesun", "ALTER.lejiang")));
  }

  @Test
  public void testShowGrantsForDatabase() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetAclPoliciesResponseBody(
                mockAclPolicy("TABLE_CREATOR", "sraikar"),
                mockAclPolicy("TABLE_CREATOR", "lesun"))));
    List<String> actualRows =
        spark.sql("SHOW GRANTS ON DATABASE openhouse.db").collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        actualRows.containsAll(ImmutableList.of("CREATE TABLE.sraikar", "CREATE TABLE.lesun")));
  }

  @Test
  public void testShowGrantsForTablePublicUser() {
    mockTableService.enqueue(
        mockResponse(200, mockGetAclPoliciesResponseBody(mockAclPolicy("TABLE_VIEWER", "*"))));
    List<String> actualRows =
        spark.sql("SHOW GRANTS ON TABLE openhouse.db.table").collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(actualRows.containsAll(ImmutableList.of("SELECT.PUBLIC")));
  }

  @Test
  public void testBadRequestServerErrorDb() {
    mockTableService.enqueue(
        mockResponse(
            400,
            "{\"status\":\"BAD_REQUEST\",\"error\":\"Bad Request\",\"message\":\"db is not a shared database\"}"));
    String ddlWithSchema = "GRANT CREATE TABLE ON DATABASE openhouse.db TO sraikar";
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(exception.getMessage().contains("db is not a shared database"));
  }

  @Test
  public void testBadRequestServerError() {
    mockTableService.enqueue(
        mockResponse(
            400,
            "{\"status\":\"BAD_REQUEST\",\"error\":\"Bad Request\",\"message\":\"db.tb1 is not a shared table\"}"));
    String ddlWithSchema = "GRANT SELECT ON TABLE openhouse.dgrant.t1 TO sraikar";
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(exception.getMessage().contains("db.tb1 is not a shared table"));
  }

  @Test
  public void testOtherServerError() {
    mockTableService.enqueue(
        mockResponse(
            403,
            "{\"status\":\"FORBIDDEN\",\"error\":\"forbidden\",\"message\":\"Operation on table db.tb1 failed as user sraikar is unauthorized\"}"));
    String ddlWithSchema = "GRANT SELECT ON TABLE openhouse.dgrant.t1 TO sraikar";
    WebClientResponseWithMessageException exception =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Operation on table db.tb1 failed as user sraikar is unauthorized"));
    Assertions.assertEquals(exception.getStatusCode(), HttpStatus.FORBIDDEN.value());

    mockTableService.enqueue(
        mockResponse(
            500,
            "{\"status\":\"INTERNAL_SERVER_ERROR\",\"error\":\"Internal Server Error\",\"message\":\"Something went wrong on the server\"}"));
    exception =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(exception.getMessage().contains("Something went wrong on the server"));
    Assertions.assertEquals(exception.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR.value());

    // Test empty response
    mockTableService.enqueue(new MockResponse().setResponseCode(401));
    exception =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class, () -> spark.sql(ddlWithSchema));
    Assertions.assertTrue(exception.getMessage().equals("401 Unauthorized"));
    Assertions.assertEquals(exception.getStatusCode(), HttpStatus.UNAUTHORIZED.value());
  }

  private Dispatcher assertDispatcher(UpdateAclPoliciesRequestBody expectedRequestBody) {
    return new Dispatcher() {
      @NotNull
      @Override
      public MockResponse dispatch(@NotNull RecordedRequest recordedRequest)
          throws InterruptedException {
        UpdateAclPoliciesRequestBody providedRequestBody =
            new Gson()
                .fromJson(recordedRequest.getBody().readUtf8(), UpdateAclPoliciesRequestBody.class);
        Assertions.assertEquals(providedRequestBody, expectedRequestBody);
        return mockResponse(200, "");
      }
    };
  }

  private UpdateAclPoliciesRequestBody getUpdateAclPoliciesRequestBody(
      String grant, String principal, String role) {
    UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody = new UpdateAclPoliciesRequestBody();
    updateAclPoliciesRequestBody.setOperation(
        UpdateAclPoliciesRequestBody.OperationEnum.fromValue(grant));
    updateAclPoliciesRequestBody.setPrincipal(principal);
    updateAclPoliciesRequestBody.setRole(role);
    return updateAclPoliciesRequestBody;
  }
}
