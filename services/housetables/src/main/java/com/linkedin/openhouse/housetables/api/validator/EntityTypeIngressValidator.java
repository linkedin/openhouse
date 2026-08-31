package com.linkedin.openhouse.housetables.api.validator;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.model.EntityType;
import org.springframework.stereotype.Component;

/**
 * Resolves the entity type of a PUT payload at ingress, ahead of every other validation. The wire
 * field stays nullable for rolling compatibility: a payload may agree with its route or stay
 * silent, never override it.
 *
 * <p>Returns an outcome rather than throwing, so the controller owns the status that says so.
 */
@Component
public class EntityTypeIngressValidator {

  public static final String EMPTY_ENTITY_MESSAGE = "entity cannot be empty";

  public static final String TYPE_MISMATCH_MESSAGE_FORMAT =
      "entityType provided: %s, but this endpoint serves %s only";

  /**
   * @param userTable the payload entity, which may be absent
   * @return the payload stamped with the route's canonical type, or the reason it could not be
   */
  public EntityTypeIngressResult normalize(UserTable userTable, EntityType routeEntityType) {
    if (userTable == null) {
      return EntityTypeIngressResult.failure(EMPTY_ENTITY_MESSAGE);
    }
    String declaredEntityType = userTable.getEntityType();
    if (declaredEntityType != null
        && !declaredEntityType.equalsIgnoreCase(routeEntityType.name())) {
      return EntityTypeIngressResult.failure(
          String.format(TYPE_MISMATCH_MESSAGE_FORMAT, declaredEntityType, routeEntityType.name()));
    }
    return EntityTypeIngressResult.success(
        userTable.toBuilder().entityType(routeEntityType.name()).build());
  }
}
