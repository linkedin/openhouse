package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.NamespaceRow;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NamespaceHtsJdbcRepository extends JpaRepository<NamespaceRow, String> {
  Optional<NamespaceRow> findByNamespaceName(String namespaceName);
}
