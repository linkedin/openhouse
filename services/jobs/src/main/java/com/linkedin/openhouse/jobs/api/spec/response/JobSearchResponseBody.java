package com.linkedin.openhouse.jobs.api.spec.response;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
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
public class JobSearchResponseBody {
  @Schema(description = "List of jobs matching the search criteria")
  private List<JobResponseBody> results;

  @Schema(description = "Total count of jobs matching the criteria")
  private Long totalCount;

  @Schema(description = "Number of results skipped")
  private Integer offset;

  @Schema(description = "Maximum number of results returned")
  private Integer limit;
}
