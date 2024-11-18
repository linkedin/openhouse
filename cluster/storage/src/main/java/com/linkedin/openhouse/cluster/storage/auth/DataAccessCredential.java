package com.linkedin.openhouse.cluster.storage.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DataAccessCredential {

  @Schema(
      description = "Map with the access credentials",
      example = "{'token':'header.payload.signature', 'path':'/my/table'}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Map<String, String> credential;

  @Schema(description = "Expiration date of token in millis since epoch", example = "0")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long expirationMillisSinceEpoch;
}
