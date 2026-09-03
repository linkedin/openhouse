package com.linkedin.openhouse.housetables.e2e.fixture;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Whole-store teardown for the H2 user-table store. Exists because {@code
 * UserTableHtsJdbcRepository} seals every inherited delete including no-arg {@code deleteAll()}, so
 * the only caller that ever wanted one is the one that now owns it: the tests.
 */
@Component
public class UserTableStoreCleaner {

  @PersistenceContext private EntityManager entityManager;

  /** Removes every row regardless of discriminator, so a corrupt row cannot survive a teardown. */
  @Transactional
  public void clear() {
    entityManager.createQuery("DELETE FROM UserTableRow").executeUpdate();
  }
}
