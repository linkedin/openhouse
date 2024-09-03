package com.linkedin.openhouse.internal.catalog.toggle.repository;

import com.linkedin.openhouse.internal.catalog.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.internal.catalog.toggle.model.ToggleStatusKey;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ToggleStatusesRepository
    extends CrudRepository<TableToggleStatus, ToggleStatusKey> {}
