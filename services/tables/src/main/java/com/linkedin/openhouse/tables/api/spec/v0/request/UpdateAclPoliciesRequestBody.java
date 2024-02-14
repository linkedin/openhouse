package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.GsonBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UpdateAclPoliciesRequestBody {
  @Schema(description = "Role that is being granted/revoked.")
  @NotEmpty(message = "role cannot be empty")
  String role;

  @Schema(description = "Grantee principal whose role is being updated")
  @NotEmpty(message = "principal cannot be empty")
  String principal;

  @Schema(description = "Whether this is a grant/revoke request", example = "GRANT")
  @NotNull(message = "operation value is incorrect")
  @Valid
  Operation operation;

  public enum Operation {
    GRANT,
    REVOKE
  }

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
