package com.linkedin.openhouse.housetables.api.spec.request;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CreateDatabaseRequestBody {
  @Schema(required = true, description = "Logical database name.")
  @NotEmpty
  @Size(max = 128)
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX)
  private String databaseName;

  @Schema(description = "Initial properties. Omitted properties default to an empty map.")
  @NotNull
  @Builder.Default
  private Map<
          @NotNull @Size(max = 512)
          @Pattern(regexp = "\\P{Cs}*", message = "must contain well-formed Unicode") String,
          @NotNull String>
      properties = Collections.emptyMap();
}
