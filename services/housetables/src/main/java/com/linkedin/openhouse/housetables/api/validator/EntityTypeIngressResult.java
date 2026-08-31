package com.linkedin.openhouse.housetables.api.validator;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import java.util.Optional;

/**
 * The outcome of normalizing a PUT payload against the type its route owns. A value rather than an
 * exception, so the decision to answer 400 stays with the transport layer that owns that status.
 */
public final class EntityTypeIngressResult {

  private final UserTable normalizedEntity;

  private final String failureMessage;

  private EntityTypeIngressResult(UserTable normalizedEntity, String failureMessage) {
    this.normalizedEntity = normalizedEntity;
    this.failureMessage = failureMessage;
  }

  public static EntityTypeIngressResult success(UserTable normalizedEntity) {
    if (normalizedEntity == null) {
      throw new IllegalArgumentException("a successful normalization must carry an entity");
    }
    return new EntityTypeIngressResult(normalizedEntity, null);
  }

  public static EntityTypeIngressResult failure(String failureMessage) {
    if (failureMessage == null) {
      throw new IllegalArgumentException("a failed normalization must carry a message");
    }
    return new EntityTypeIngressResult(null, failureMessage);
  }

  public boolean isSuccess() {
    return failureMessage == null;
  }

  public Optional<UserTable> getNormalizedEntity() {
    return Optional.ofNullable(normalizedEntity);
  }

  public Optional<String> getFailureMessage() {
    return Optional.ofNullable(failureMessage);
  }
}
