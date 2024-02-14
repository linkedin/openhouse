package com.linkedin.openhouse.hts.catalog.model.jobtable;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

@Builder
@Getter
public class JobIcebergRow implements IcebergRow {

  private String jobId;

  private String state;

  private String version;

  private String jobName;

  private String clusterId;

  private Long creationTimeMs;

  private Long startTimeMs;

  private Long finishTimeMs;

  private Long lastUpdateTimeMs;

  private String jobConf;

  private Long heartbeatTimeMs;

  private String executionId;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "jobId", Types.StringType.get()),
        Types.NestedField.required(2, "state", Types.StringType.get()),
        Types.NestedField.required(3, "version", Types.StringType.get()),
        Types.NestedField.required(4, "jobName", Types.StringType.get()),
        Types.NestedField.required(5, "clusterId", Types.StringType.get()),
        Types.NestedField.optional(6, "creationTimeMs", Types.LongType.get()),
        Types.NestedField.optional(7, "startTimeMs", Types.LongType.get()),
        Types.NestedField.optional(8, "finishTimeMs", Types.LongType.get()),
        Types.NestedField.optional(9, "lastUpdateTimeMs", Types.LongType.get()),
        Types.NestedField.optional(10, "jobConf", Types.StringType.get()),
        Types.NestedField.optional(11, "heartbeatTimeMs", Types.LongType.get()),
        Types.NestedField.optional(12, "executionId", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("jobId", jobId);
    genericRecord.setField("state", state);
    genericRecord.setField("version", version);
    genericRecord.setField("jobName", jobName);
    genericRecord.setField("clusterId", clusterId);
    genericRecord.setField("creationTimeMs", creationTimeMs);
    genericRecord.setField("startTimeMs", startTimeMs);
    genericRecord.setField("finishTimeMs", finishTimeMs);
    genericRecord.setField("lastUpdateTimeMs", lastUpdateTimeMs);
    genericRecord.setField("jobConf", jobConf);
    genericRecord.setField("heartbeatTimeMs", heartbeatTimeMs);
    genericRecord.setField("executionId", executionId);
    return genericRecord;
  }

  @Override
  public String getVersionColumnName() {
    return "version";
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return JobIcebergRowPrimaryKey.builder().jobId(jobId).build();
  }
}
