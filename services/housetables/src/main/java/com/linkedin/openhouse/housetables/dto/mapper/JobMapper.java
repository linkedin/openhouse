package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRowPrimaryKey;
import java.util.Map;
import org.mapstruct.Mapper;

/**
 * Mapper class to transform between {@link com.linkedin.openhouse.housetables.dto.model.JobDto} and
 * Data Model objects.
 */
@Mapper(componentModel = "spring")
public interface JobMapper {

  Job toJob(JobDto jobDto);

  Job mapToJob(Map<String, String> properties);

  JobDto toJobDto(JobRow jobRow);

  JobDto toJobDto(Job job);

  JobRowPrimaryKey toJobRowPrimaryKey(Job job);

  JobRow toJobRow(Job job);

  JobRowPrimaryKey toJobRowPrimaryKey(JobRow jobRow);

  JobIcebergRow toJobIcebergRow(JobRow jobRow);

  JobIcebergRowPrimaryKey toJobIcebergRowPrimaryKey(JobRowPrimaryKey jobRowPrimaryKey);

  JobRow toJobRow(JobIcebergRow jobIcebergRow);
}
