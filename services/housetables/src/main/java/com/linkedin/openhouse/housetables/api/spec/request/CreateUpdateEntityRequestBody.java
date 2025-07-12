package com.linkedin.openhouse.housetables.api.spec.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.openhouse.housetables.util.GsonBuilderWithLocalDateTimeAdapter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CreateUpdateEntityRequestBody<T> {
  @Schema(
      description =
          "The entity object that clients want to create/update in the target house table.",
      required = true)
  @JsonProperty(value = "entity")
  T entity;

  public String toJson() {
    return GsonBuilderWithLocalDateTimeAdapter.createGson().toJson(this);
  }
}
