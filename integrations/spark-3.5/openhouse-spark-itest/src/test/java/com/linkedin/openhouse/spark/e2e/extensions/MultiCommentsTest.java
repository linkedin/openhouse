package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.gson.Gson;
import com.linkedin.openhouse.gen.tables.client.model.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.spark.SparkTestBase;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class MultiCommentsTest {

  @Test
  public void testGrantStatement() {
    mockTableService.setDispatcher(
        assertDispatcher(getUpdateAclPoliciesRequestBody("GRANT", "sraikar", "TABLE_VIEWER")));
    String ddlWithSchema =
        String.join(
            "\n",
            "/* sameline comment */",
            "-- another sameline comment",
            "/* some multiline",
            " comment */",
            "GRANT  /* in-statement comment */ SELECT ON TABLE openhouse.dgrant.t1 to sraikar -- inline comment");
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
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
