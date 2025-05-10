package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * Interface for repository backed by Iceberg/JDBC for storing and retrieving {@link UserTable} or
 * {@link com.linkedin.openhouse.housetables.api.spec.model.Job} row object.
 */
public interface HtsRepository<T, ID> extends PagingAndSortingRepository<T, ID> {}
