package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class GetDataAccessCredentialResponseBody {

  @Schema(
      description = "Map with the access credentials",
      example = "{'token':'header.payload.signature', 'path':'/my/table'}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Map<String, String> credential;

  @Schema(description = "Expiration date of token in millis since epoch", example = "0")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long expirationMillisSinceEpoch;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
