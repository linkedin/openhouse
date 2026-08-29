package com.linkedin.openhouse.housetables.api.validator;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import java.util.Optional;

/**
 * The outcome of normalizing a PUT payload against the type its route owns. An explicit result
 * rather than an exception, so the ingress rule is a value the controller inspects and the decision
 * to answer 400 stays with the transport layer that owns that status.
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

  /** Present exactly when {@link #isSuccess()}. */
  public Optional<UserTable> getNormalizedEntity() {
    return Optional.ofNullable(normalizedEntity);
  }

  /** Present exactly when {@link #isSuccess()} is false. */
  public Optional<String> getFailureMessage() {
    return Optional.ofNullable(failureMessage);
  }
}
