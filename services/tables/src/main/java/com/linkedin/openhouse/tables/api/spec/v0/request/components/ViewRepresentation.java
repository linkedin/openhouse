package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * ViewRepresentation is the entity holding a single engine-specific representation of a view in the
 * /views API request body. SQL text is stored opaquely; this class carries no parsing, translation
 * or dialect-support logic. Byte-size, representation-type and dialect-support rules are owned by
 * the manual view validator.
 */
@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ViewRepresentation {

  @Schema(description = "Type of the view representation", example = "sql")
  @NotEmpty(message = "type cannot be empty")
  private String type;

  @Schema(
      description = "Opaque SQL text of the view representation. The server never parses it.",
      example = "SELECT id, name FROM my_database.my_table")
  @NotEmpty(message = "sql cannot be empty")
  private String sql;

  @Schema(description = "SQL dialect the representation is written in", example = "spark")
  @NotEmpty(message = "dialect cannot be empty")
  private String dialect;
}
