package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.tables.model.ViewDto;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Internal, never-serialized carrier for the result of a create/replace commit.
 *
 * <p>{@link ViewDto} has no UUID field (the pointer-only read contract deliberately omits it), but
 * the create-operation audit must record the view UUID the engine assigned. The UUID exists only in
 * the engine's {@code ViewCommitResult.viewUuid}, and on create there is no captured row to read it
 * from afterward. This carrier threads that value from {@code
 * OpenHouseInternalViewRepository#commitCreate}/{@code #commitReplace} to the service's audit
 * emission, without a reread and without parsing the UUID out of the metadata path.
 *
 * <p>Lives in the repository package: it is an internal repository-to-service boundary type, not a
 * wire/response type. {@link #getDto()} is the unchanged public service/wire response shape; {@link
 * #getCommittedViewUuid()} and {@link #isCreated()} exist only for audit emission and the public
 * {@code Pair<ViewDto, Boolean>} status selection.
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
