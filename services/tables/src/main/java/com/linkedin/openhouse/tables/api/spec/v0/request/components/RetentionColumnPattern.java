package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * An optional subfield as part of {@link Retention} so that user can specify column name and value
 * pattern when dealing with tables like those are partitioned by a string-type column with
 * timestamp semantic.
 */
@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RetentionColumnPattern {
  @Schema(description = "Name of retention column", example = "datepartition")
  @NotEmpty(message = "columnName cannot be empty")
  @NotNull(message = "columnName cannot be null")
  String columnName;

  @Schema(
      description =
          "Pattern for the value of the retention column following java.time.format.DateTimeFormatter standard. "
              + "Defaults to 'yyyy-MM-dd' day format if empty.",
      example = "yyyy-MM-dd-HH")
  @NotNull(message = "pattern cannot be null")
  String pattern;
}
