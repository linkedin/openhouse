package com.linkedin.openhouse.housetables.api.validator;

/** Interface defining validations for all /hts (housetables) REST endpoints. */
public interface HouseTablesApiValidator<K, V> {

  /**
   * Function to validate a request to get a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to fetch.
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
   */
  void validateGetEntity(K key);

  /**
   * Function to validate a request to delete a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to delete.
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
   */
  void validateDeleteEntity(K key);

  /**
   * Function to validate a request to get all rows that matches the given entity.
   *
   * @param entity The complete entity to be retrieved from House table.
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
   */
  void validateGetEntities(V entity);

  /**
   * Function to validate a request for upsert of a row in a House Table.
   *
   * @param entity The complete entity to be upsert-ed into target House table.
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
   */
  void validatePutEntity(V entity);
}
