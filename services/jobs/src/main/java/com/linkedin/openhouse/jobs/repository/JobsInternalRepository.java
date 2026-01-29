package com.linkedin.openhouse.jobs.repository;

import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.model.JobDtoPrimaryKey;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobsInternalRepository extends CrudRepository<JobDto, JobDtoPrimaryKey> {
  List<JobDto> findByJobNameStartingWith(String prefix, Pageable pageable);
}
