package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Base data manager abstract class internally uses blocking queue to manage data represented by
 * type DATA in the context of a multi-threaded environment where data is produced and consumed by
 * multiple threads in parallel. Provides utility methods to add data and consume data. It provides
 * method to signal data generation completion and additional methods for data size empty check etc.
 *
 * @param <DATA>
 */
@Slf4j
public abstract class BaseDataManager<DATA> {
  private BlockingQueue<DATA> dataQueue;
  private AtomicBoolean dataGenerationCompleted;
  private AtomicLong totalDataCount;
  private JobConf.JobTypeEnum jobType;
  private final int QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT = 1;

  public BaseDataManager(JobConf.JobTypeEnum jobType) {
    this.jobType = jobType;
    dataQueue = new LinkedBlockingQueue<>();
    dataGenerationCompleted = new AtomicBoolean(false);
    totalDataCount = new AtomicLong(0);
  }

  /*public BlockingQueue<DATA> getDataQueue() {
    return dataQueue;
  }*/

  /**
   * Adds data to the internal blocking queue
   *
   * @param data
   * @throws InterruptedException
   */
  public void addData(DATA data) throws InterruptedException {
    dataQueue.put(data);
    totalDataCount.incrementAndGet();
  }

  /** Signal data generation completion */
  public void updateDataGenerationCompletion() {
    dataGenerationCompleted.set(true);
  }

  /**
   * if data generation completed or not
   *
   * @return boolean
   */
  public boolean isDataGenerationCompleted() {
    return dataGenerationCompleted.get();
  }

  /**
   * Polls the internal blocking queue for data. Returns null if there is no data or on polling
   * timeout.
   *
   * @return
   * @throws InterruptedException
   */
  public DATA getData() throws InterruptedException {
    DATA data = dataQueue.poll(QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT, TimeUnit.MINUTES);
    if (data == null && !hasNext()) {
      log.info("No more data available in the queue for job type {}", jobType);
    }
    return data;
  }

  /**
   * If the internal queue has data or not
   *
   * @return boolean
   */
  public synchronized boolean hasNext() {
    return !(dataGenerationCompleted.get() && dataQueue.isEmpty());
  }

  /** Clears the internal queue */
  public void clear() {
    dataQueue.clear();
  }

  /**
   * Is the internal queue empty
   *
   * @return boolean
   */
  public synchronized boolean isEmpty() {
    return dataQueue.isEmpty();
  }

  /**
   * Keeps track of the total data size
   *
   * @return long
   */
  public long getTotalDataCount() {
    return totalDataCount.get();
  }

  /**
   * Provides the current data size in the internal queue
   *
   * @return
   */
  public synchronized long getCurrentDataCount() {
    return dataQueue.size();
  }

  /** Resets all the fields */
  public void resetAll() {
    dataQueue.clear();
    dataGenerationCompleted.set(false);
    totalDataCount.set(0);
  }
}
