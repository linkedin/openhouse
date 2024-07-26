package com.linkedin.openhouse.cluster.storage.hdfs;

import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The HdfsStorage class is an implementation of the Storage interface for HDFS storage. It uses a
 * HdfsStorageClient to interact with the HDFS file system. The HdfsStorageClient uses the {@link
 * org.apache.hadoop.fs.FileSystem} class to interact with the HDFS file system.
 */
@Component
public class HdfsStorage extends BaseStorage {

  @Autowired @Lazy private HdfsStorageClient hdfsStorageClient;

  /**
   * Get the type of the HDFS storage.
   *
   * @return the type of the HDFS storage
   */
  @Override
  public StorageType.Type getType() {
    return StorageType.HDFS;
  }

  /**
   * Get the HDFS storage client.
   *
   * @return the HDFS storage client
   */
  @Override
  public StorageClient<?> getClient() {
    return hdfsStorageClient;
  }

  /**
   * Allocates Table Space for the HDFS storage.
   *
   * <p>tableLocation looks like: /{rootPrefix}/{databaseId}/{tableId}-{tableUUID} We strip the
   * endpoint to ensure backward-compatibility. This override should be removed after resolving <a
   * href="https://github.com/linkedin/openhouse/issues/121">
   *
   * @return the table location
   */
  @Override
  public String allocateTableLocation(
      String databaseId, String tableId, String tableUUID, String tableCreator) {
    return Paths.get(getClient().getRootPrefix(), databaseId, tableId + "-" + tableUUID).toString();
  }

  /**
   * Deallocates/deletes Table Storage location recursively for Hdfs storage
   *
   * @param location the base location of the table
   * @param tableCreator the creator of the table
   * @throws IOException
   */
  @Override
  public void deallocateTableLocation(String location, String tableCreator) throws IOException {
    this.hdfsStorageClient.getNativeClient().delete(new Path(location), true);
  }
}
