package com.linkedin.openhouse.housetables.api.spec.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.openhouse.housetables.util.GsonBuilderWithLocalDateTimeAdapter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

/**
 * A generic class for response body of methods that deals with key'ed entity in different types of
 * House tables.
 *
 * @param <T> Representing the entity type.
 */
@Builder
public class EntityResponseBody<T> {

  @Schema(description = "Representing a row in the house table ", required = true)
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Getter
  protected T entity;

  public String toJson() {
    return GsonBuilderWithLocalDateTimeAdapter.createGson().toJson(this);
  }
}
