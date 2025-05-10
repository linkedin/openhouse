package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;

/**
 * Interface layer between REST and HouseTables backend. The implementation is injected into the
 * Service Controller. Invocation of this class should provide the entity type. Should avoid raw
 * usage of this interface.
 *
 * @param <K> The generic type to represent key type of the row-object.
 * @param <V> The generic type to represent value type of the row-object from House table.
 */
public interface HouseTablesApiHandler<K, V> {
  /**
   * Function to GET a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to obtain.
   * @return the row as part of response body that would be returned to the client.
   */
  ApiResponse<EntityResponseBody<V>> getEntity(K key);

  // TODO: Generalize this to share among all house tables?

  /**
   * Function to get all rows that fulfills the given entity.
   *
   * @param entity The entity serves as a container for predicates.
   * @return all rows that fulfills the predicates expressed through param entity.
   */
  ApiResponse<GetAllEntityResponseBody<V>> getEntities(V entity);

  /**
   * Function to get paginated rows that fulfills the given entity.
   *
   * @param entity The entity serves as a container for predicates.
   * @param page The page number to be retrieved
   * @param size The number of entities in the page
   * @param sortBy The results sorted by a field in the entity. For example, tableId, databaseId
   * @return Paginated rows that fulfills the predicates expressed through param entity.
   */
  ApiResponse<GetAllEntityResponseBody<V>> getEntities(V entity, int page, int size, String sortBy);

  /**
   * Function to Delete a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to delete.
   * @return the row as part of response body that would be returned to the client.
   */
  ApiResponse<Void> deleteEntity(K key);

  /**
   * Function to update or insert a row in a House Table.
   *
   * @param entity The complete entity to be upsert-ed into target House table.
   * @return the row as part of response body that would be returned to the client.
   */
  ApiResponse<EntityResponseBody<V>> putEntity(V entity);
}
