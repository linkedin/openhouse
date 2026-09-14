package com.linkedin.openhouse.tables.model;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * One page of a view listing as the view service returns it: the views it found, and the token a
 * client sends back to continue.
 *
 * <p>The token is opaque here. Nothing in the API generates, parses, signs or expires it, and the
 * ordering, snapshot and replay semantics of a traversal belong to the service that produces it.
 *
 * <p>A null token means the listing is complete, and it is the only completion signal: an empty or
 * short {@code results} list does not mean the caller should stop. {@code results} is required, so
 * a service with nothing to return builds an explicit empty list rather than leaving it unset.
 */
@Builder
@Value
public class ViewListResult {

  @NonNull private List<ViewDto> results;

  private String nextPageToken;
}
