package com.linkedin.openhouse.housetables.api.spec.response;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class UpdateDatabasePropertiesResponseBody {
  @Schema(required = true, description = "Keys supplied in updates, including unchanged values.")
  List<String> updated;

  @Schema(required = true, description = "Keys that existed and were removed.")
  List<String> removed;

  @Schema(required = true, description = "Removal keys that were already absent.")
  List<String> missing;
}
