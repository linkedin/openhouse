package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.tables.model.ViewDto;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The view commit result that {@link OpenHouseInternalViewRepository} returns to ViewsServiceImpl.
 * Not the engine's {@code ViewCommitResult}, and never serialized.
 *
 * <p>It is shared instead of passing just a {@link ViewDto} because the service needs two values
 * the DTO cannot carry: the committed view UUID, which audit records but the pointer-only response
 * omits, and whether the commit created the view, which selects the response status.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewCommitOutcome {

  private final ViewDto dto;

  private final String committedViewUuid;

  private final boolean created;
}
