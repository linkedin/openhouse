package com.linkedin.openhouse.hts.catalog.model.jobtable;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

@Builder
@Getter
public class JobIcebergRowPrimaryKey implements IcebergRowPrimaryKey {

  private String jobId;

  @Override
  public Schema getSchema() {
    return new Schema(Types.NestedField.required(1, "jobId", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("jobId", jobId);
    return genericRecord;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.equal("jobId", jobId);
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    return JobIcebergRow.builder()
        .jobId((String) record.getField("jobId"))
        .state((String) record.getField("state"))
        .version((String) record.getField("version"))
        .jobName((String) record.getField("jobName"))
        .clusterId((String) record.getField("clusterId"))
        .creationTimeMs((Long) record.getField("creationTimeMs"))
        .startTimeMs((Long) record.getField("startTimeMs"))
        .finishTimeMs((Long) record.getField("finishTimeMs"))
        .lastUpdateTimeMs((Long) record.getField("lastUpdateTimeMs"))
        .jobConf((String) record.getField("jobConf"))
        .heartbeatTimeMs((Long) record.getField("heartbeatTimeMs"))
        .executionId((String) record.getField("executionId"))
        .retentionTimeSec((Long) record.getField("retentionTimeSec"))
        .build();
  }
}
