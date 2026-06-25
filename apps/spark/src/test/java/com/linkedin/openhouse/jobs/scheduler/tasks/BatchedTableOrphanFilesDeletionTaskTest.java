package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadataBatch;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class BatchedTableOrphanFilesDeletionTaskTest {

  @Test
  public void operationTypeIsBatchedOfd() {
    Assertions.assertEquals(
        JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH,
        BatchedTableOrphanFilesDeletionTask.OPERATION_TYPE);
  }

  @Test
  public void getArgsBuildsCsvOfFqtns() {
    TableMetadataBatch batch =
        TableMetadataBatch.builder()
            .dbName("db")
            .tables(Arrays.asList(table("db", "t1"), table("db", "t2"), table("db", "t3")))
            .build();
    BatchedTableOrphanFilesDeletionTask task =
        new BatchedTableOrphanFilesDeletionTask(
            Mockito.mock(JobsClient.class), Mockito.mock(TablesClient.class), batch);

    Assertions.assertEquals(Arrays.asList("--tableNames", "db.t1,db.t2,db.t3"), task.getArgs());
  }

  @Test
  public void shouldRunFalseForEmptyBatch() {
    TableMetadataBatch batch =
        TableMetadataBatch.builder().dbName("db").tables(Collections.emptyList()).build();
    BatchedTableOrphanFilesDeletionTask task =
        new BatchedTableOrphanFilesDeletionTask(
            Mockito.mock(JobsClient.class), Mockito.mock(TablesClient.class), batch);

    Assertions.assertFalse(task.shouldRun());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void launchJobUsesBatchScopedJobName() {
    JobsClient jobsClient = Mockito.mock(JobsClient.class);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of("job-42"));

    TableMetadataBatch batch =
        TableMetadataBatch.builder()
            .dbName("warehouse")
            .tables(Arrays.asList(table("warehouse", "a"), table("warehouse", "b")))
            .build();
    BatchedTableOrphanFilesDeletionTask task =
        new BatchedTableOrphanFilesDeletionTask(
            jobsClient, Mockito.mock(TablesClient.class), batch);

    boolean launched = task.launchJob();

    Assertions.assertTrue(launched);
    Assertions.assertEquals("job-42", task.getJobId());
    ArgumentCaptor<String> jobNameCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(jobsClient)
        .launch(
            jobNameCaptor.capture(),
            Mockito.eq(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH),
            Mockito.anyString(),
            Mockito.anyMap(),
            Mockito.anyList());
    // Job name carries db + batch size; nothing per-table is embedded so the string stays bounded.
    Assertions.assertEquals("ORPHAN_FILES_DELETION_BATCH_warehouse_2", jobNameCaptor.getValue());
  }

  private static TableMetadata table(String db, String name) {
    return TableMetadata.builder().dbName(db).tableName(name).creator("test-user").build();
  }
}
