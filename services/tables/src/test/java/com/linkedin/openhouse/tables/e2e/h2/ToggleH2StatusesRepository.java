package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.internal.catalog.toggle.repository.ToggleStatusesRepository;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

/**
 * The {@link org.springframework.context.annotation.Bean} injected into /tables e2e tests when
 * communication to the implementation of {@link ToggleStatusesRepository} is not needed. With
 * {@link Primary} annotation, this repository will be the default injection.
 */
@Repository
// @Primary
public interface ToggleH2StatusesRepository extends ToggleStatusesRepository {}
