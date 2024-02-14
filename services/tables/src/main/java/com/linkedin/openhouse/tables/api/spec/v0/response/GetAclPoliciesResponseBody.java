package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class GetAclPoliciesResponseBody {
  @Schema(description = "List of acl policies associated with table/database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<AclPolicy> results;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
