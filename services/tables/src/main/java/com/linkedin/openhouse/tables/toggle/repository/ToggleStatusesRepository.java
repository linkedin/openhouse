package com.linkedin.openhouse.tables.toggle.repository;

import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ToggleStatusesRepository
    extends CrudRepository<TableToggleStatus, ToggleStatusKey> {}
