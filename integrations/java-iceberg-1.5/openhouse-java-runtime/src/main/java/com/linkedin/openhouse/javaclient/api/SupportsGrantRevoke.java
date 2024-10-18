package com.linkedin.openhouse.javaclient.api;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsGrantRevoke {

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  class AclPolicyDto {
    String privilege;
    String principal;
  }

  /**
   * updateAclPolicies for an OH table.
   *
   * <p>The following SQL command: GRANT [privilege] ON TABLE [db.table] TO [principal]
   *
   * <p>can be converted into following parameters
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @param isGrant if GRANT statement then true, if REVOKE statement then false, ex: true
   * @param privilege ex: SELECT, DESCRIBE
   * @param principal ex: sraikar
   */
  void updateTableAclPolicies(
      TableIdentifier tableIdentifier, boolean isGrant, String privilege, String principal);

  /**
   * get AclPolicies for an OH table.
   *
   * <p>The following SQL command: SHOW GRANTS ON TABLE [db.table]
   *
   * <p>can be converted into following parameters
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @return ListOf{@link AclPolicyDto} containing all principals and their privileges
   */
  List<AclPolicyDto> getTableAclPolicies(TableIdentifier tableIdentifier);

  /**
   * updateAclPolicies for an OH database.
   *
   * <p>The following SQL command: GRANT [privilege] ON DATABASE [db] TO [principal]
   *
   * <p>can be converted into following parameters
   *
   * @param namespace identifier for the database, ex: db
   * @param isGrant if GRANT statement then true, if REVOKE statement then false, ex: true
   * @param privilege ex: CREATE TABLE
   * @param principal ex: sraikar
   */
  void updateDatabaseAclPolicies(
      Namespace namespace, boolean isGrant, String privilege, String principal);

  /**
   * get AclPolicies for an OH database.
   *
   * <p>The following SQL command: SHOW GRANTS ON DATABASE [db]
   *
   * <p>can be converted into following parameters
   *
   * @param namespace identifier for the database, ex: db
   * @return ListOf{@link AclPolicyDto} containing all principals and their privileges
   */
  List<AclPolicyDto> getDatabaseAclPolicies(Namespace namespace);
}
