package com.linkedin.openhouse.housetables.api.validator;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.model.EntityType;
import org.springframework.stereotype.Component;

/**
 * Resolves the entity type of a PUT payload at ingress, ahead of every other validation. The wire
 * field stays nullable for rolling compatibility: a payload may agree with its route or stay
 * silent, never override it.
 */
@Component
public class EntityTypeIngressValidator {

  public static final String EMPTY_ENTITY_MESSAGE = "entity cannot be empty";

  public static final String TYPE_MISMATCH_MESSAGE_FORMAT =
      "entityType provided: %s, but this endpoint serves %s only";

  /**
   * @param userTable the payload entity, which may be absent
   * @return the payload stamped with the route's canonical type
   * @throws RequestValidationFailureException the payload is absent, or names a type its route does
   *     not serve
   */
  public UserTable normalize(UserTable userTable, EntityType routeEntityType) {
    if (userTable == null) {
      throw new RequestValidationFailureException(EMPTY_ENTITY_MESSAGE);
    }
    String declaredEntityType = userTable.getEntityType();
    if (declaredEntityType != null
        && !declaredEntityType.equalsIgnoreCase(routeEntityType.name())) {
      throw new RequestValidationFailureException(
          String.format(TYPE_MISMATCH_MESSAGE_FORMAT, declaredEntityType, routeEntityType.name()));
    }
    return userTable.toBuilder().entityType(routeEntityType.name()).build();
  }
}
