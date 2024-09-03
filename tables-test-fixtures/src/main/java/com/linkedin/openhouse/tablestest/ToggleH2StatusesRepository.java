package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.toggle.repository.ToggleStatusesRepository;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

/** Repository based on in-memory storage require to ensure test suites works as expected. */
@Repository
@Primary
public interface ToggleH2StatusesRepository extends ToggleStatusesRepository {}
