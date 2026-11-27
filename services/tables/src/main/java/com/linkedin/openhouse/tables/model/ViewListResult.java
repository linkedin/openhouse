package com.linkedin.openhouse.tables.model;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/** Service listing result. Only a null continuation token indicates completion. */
@Builder
@Value
public class ViewListResult {

  @NonNull private List<ViewDto> results;

  private String nextPageToken;
}
