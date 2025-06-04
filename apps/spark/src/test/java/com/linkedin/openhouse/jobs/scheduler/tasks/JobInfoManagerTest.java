package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JobInfoManagerTest {
  private JobInfoManager jobInfoManager;
  private JobInfo jobInfo1;
  private JobInfo jobInfo2;
  private JobInfo jobInfo3;
  private JobInfo jobInfo4;

  @BeforeAll
  public void setUp() {
    jobInfoManager = new JobInfoManager(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    TableMetadata tableMetadata1 = getTableMetadata(0, 0);
    String jobId1 = UUID.randomUUID().toString();
    jobInfo1 = new JobInfo(tableMetadata1, jobId1);
    TableMetadata tableMetadata2 = getTableMetadata(1, 1);
    String jobId2 = UUID.randomUUID().toString();
    jobInfo2 = new JobInfo(tableMetadata2, jobId2);
    TableMetadata tableMetadata3 = getTableMetadata(3, 3);
    String jobId3 = UUID.randomUUID().toString();
    jobInfo3 = new JobInfo(tableMetadata3, jobId3);
    TableMetadata tableMetadata4 = getTableMetadata(4, 4);
    String jobId4 = UUID.randomUUID().toString();
    jobInfo4 = new JobInfo(tableMetadata4, jobId4);
  }

  private TableMetadata getTableMetadata(int dbIndex, int tableIndex) {
    return TableMetadata.builder()
        .dbName("db" + dbIndex)
        .tableName("test_table" + tableIndex)
        .isPrimary(true)
        .build();
  }

  @Test
  public void testJobInfoManagerCase1() throws InterruptedException {
    jobInfoManager.addData(jobInfo1);
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    jobInfoManager.addData(jobInfo2);
    Assertions.assertEquals(2, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(jobInfo1, jobInfoManager.getData());
    Assertions.assertEquals(1, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(jobInfo2, jobInfoManager.getData());
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(0, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(2, jobInfoManager.getTotalDataCount());
    jobInfoManager.updateDataGenerationCompletion();
    Assertions.assertEquals(false, jobInfoManager.hasNext());
  }

  @Test
  public void testJobInfoManagerCase2() throws InterruptedException {
    jobInfoManager.addData(jobInfo1);
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    jobInfoManager.addData(jobInfo2);
    Assertions.assertEquals(2, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(jobInfo1, jobInfoManager.getData());
    Assertions.assertEquals(1, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(jobInfo2, jobInfoManager.getData());
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(0, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(2, jobInfoManager.getTotalDataCount());

    jobInfoManager.addData(jobInfo3);
    Assertions.assertEquals(true, jobInfoManager.hasNext());

    jobInfoManager.addData(jobInfo4);
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(2, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(4, jobInfoManager.getTotalDataCount());

    jobInfoManager.updateDataGenerationCompletion();
    Assertions.assertEquals(true, jobInfoManager.hasNext());
    Assertions.assertEquals(jobInfo3, jobInfoManager.getData());
    Assertions.assertEquals(1, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(jobInfo4, jobInfoManager.getData());
    Assertions.assertEquals(0, jobInfoManager.getCurrentDataCount());
    Assertions.assertEquals(4, jobInfoManager.getTotalDataCount());
    Assertions.assertEquals(false, jobInfoManager.hasNext());
  }

  @Test
  public void testRunningJobs() throws InterruptedException {
    jobInfoManager.addData(jobInfo1);
    jobInfoManager.addData(jobInfo2);
    jobInfoManager.addData(jobInfo3);
    jobInfoManager.addData(jobInfo4);
    Assertions.assertEquals(4, jobInfoManager.runningJobs.size());
    jobInfoManager.moveJobToCompletedStage(jobInfoManager.getData().getJobId());
    Assertions.assertEquals(3, jobInfoManager.runningJobs.size());
    jobInfoManager.moveJobToCompletedStage(jobInfoManager.getData().getJobId());
    Assertions.assertEquals(2, jobInfoManager.runningJobs.size());
    jobInfoManager.moveJobToCompletedStage(jobInfoManager.getData().getJobId());
    Assertions.assertEquals(1, jobInfoManager.runningJobs.size());
    jobInfoManager.moveJobToCompletedStage(jobInfoManager.getData().getJobId());
    Assertions.assertEquals(0, jobInfoManager.runningJobs.size());
  }

  @AfterEach
  public void reset() {
    jobInfoManager.resetAll();
  }
}
