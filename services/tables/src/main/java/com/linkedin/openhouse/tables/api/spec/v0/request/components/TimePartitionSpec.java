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

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TimePartitionSpec {

  @Schema(
      description =
          "Name of the timestamp column in provided schema. The column should be of the type 'Timestamp'. "
              + "Nested columns can also be provided with a dot-separated name (for example: 'eventHeader.timeColumn')."
              + "Column name is case-sensitive.",
      example = "timestampColumn")
  @NotEmpty(message = "columnName cannot be empty")
  @NotNull(message = "columnName cannot be null")
  String columnName;

  @Schema(description = "Granularity of the time partition.")
  @NotNull(message = "granularity cannot be null")
  Granularity granularity;

  @Getter
  public enum Granularity {
    HOUR("H"),
    DAY("D"),
    MONTH("M"),
    YEAR("Y");

    private final String granularity;

    Granularity(String granularity) {
      this.granularity = granularity;
    }
  }
}
