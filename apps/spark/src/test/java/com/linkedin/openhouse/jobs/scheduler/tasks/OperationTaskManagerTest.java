package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OperationTaskManagerTest {
  private OperationTaskManager operationTaskManager;
  private OperationTask<?> task1;
  private OperationTask<?> task2;
  private OperationTask<?> task3;
  private OperationTask<?> task4;

  @BeforeAll
  public void setUp() {
    operationTaskManager = new OperationTaskManager(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    task1 = Mockito.mock(TableSnapshotsExpirationTask.class);
    task2 = Mockito.mock(TableSnapshotsExpirationTask.class);
    task3 = Mockito.mock(TableSnapshotsExpirationTask.class);
    task4 = Mockito.mock(TableSnapshotsExpirationTask.class);
  }

  @Test
  public void testOperationTaskManagerCase1() throws InterruptedException {
    operationTaskManager.addData(task1);
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    operationTaskManager.addData(task2);
    Assertions.assertEquals(2, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(task1, operationTaskManager.getData());
    Assertions.assertEquals(1, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(task2, operationTaskManager.getData());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(0, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(2, operationTaskManager.getTotalDataCount());
    operationTaskManager.updateDataGenerationCompletion();
    Assertions.assertEquals(false, operationTaskManager.hasNext());
  }

  @Test
  public void testOperationTaskManagerCase2() throws InterruptedException {
    operationTaskManager.addData(task1);
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    operationTaskManager.addData(task2);
    Assertions.assertEquals(2, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(task1, operationTaskManager.getData());
    Assertions.assertEquals(1, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(task2, operationTaskManager.getData());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(0, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(2, operationTaskManager.getTotalDataCount());
    Assertions.assertEquals(true, operationTaskManager.hasNext());

    operationTaskManager.addData(task3);
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    operationTaskManager.addData(task4);
    Assertions.assertEquals(2, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(task3, operationTaskManager.getData());
    Assertions.assertEquals(1, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(task4, operationTaskManager.getData());
    Assertions.assertEquals(true, operationTaskManager.hasNext());
    Assertions.assertEquals(0, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(4, operationTaskManager.getTotalDataCount());
    operationTaskManager.updateDataGenerationCompletion();
    Assertions.assertEquals(false, operationTaskManager.hasNext());
  }

  @AfterEach
  public void reset() {
    operationTaskManager.resetAll();
  }
}
