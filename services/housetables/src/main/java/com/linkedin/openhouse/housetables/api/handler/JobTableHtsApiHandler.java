package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.model.JobKey;

/**
 * Invocation of generic type {@link HouseTablesApiHandler} using {@link Job} as the entity type.
 */
public interface JobTableHtsApiHandler extends HouseTablesApiHandler<JobKey, Job> {}
