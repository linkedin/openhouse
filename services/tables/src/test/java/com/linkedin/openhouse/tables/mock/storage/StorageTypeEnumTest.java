package com.linkedin.openhouse.tables.mock.storage;

import com.linkedin.openhouse.cluster.storage.StorageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageTypeEnumTest {

  /**
   * This test demonstrates the ability to extends the StorageType class to add new storage types.
   */
  public static class ExtendedStorageType extends StorageType {
    public static final Type GCS = new Type("gcs");
    public static final Type S3 = new Type("s3");

    @Override
    public Type fromString(String type) {
      if (type.equals(GCS.getValue())) {
        return GCS;
      } else if (type.equals(S3.getValue())) {
        return S3;
      }
      return super.fromString(type);
    }
  }

  @Test
  public void testEnumEquality() {
    Assertions.assertSame(StorageType.LOCAL, StorageType.LOCAL);
    Assertions.assertNotSame(StorageType.HDFS, StorageType.LOCAL);
    Assertions.assertNotSame(ExtendedStorageType.GCS, StorageType.HDFS);
    Assertions.assertSame(ExtendedStorageType.HDFS, StorageType.HDFS);
    Assertions.assertTrue(StorageType.LOCAL.equals(ExtendedStorageType.LOCAL));
  }

  @Test
  public void testValueEquality() {
    Assertions.assertEquals("local", StorageType.LOCAL.getValue());
    Assertions.assertEquals("hdfs", StorageType.HDFS.getValue());
    Assertions.assertEquals("gcs", ExtendedStorageType.GCS.getValue());
    Assertions.assertEquals("s3", ExtendedStorageType.S3.getValue());
    Assertions.assertEquals(StorageType.LOCAL.getValue(), ExtendedStorageType.LOCAL.getValue());
  }

  @Test
  public void testTypeFromString() {
    // Allows StorageType to be extended with new types. A primary bean can be provided.
    StorageType storageType = new ExtendedStorageType();
    Assertions.assertSame(StorageType.LOCAL, storageType.fromString("local"));
    Assertions.assertSame(StorageType.HDFS, storageType.fromString("hdfs"));
    Assertions.assertSame(ExtendedStorageType.GCS, storageType.fromString("gcs"));
    Assertions.assertSame(ExtendedStorageType.S3, storageType.fromString("s3"));
    Assertions.assertSame(StorageType.LOCAL, storageType.fromString("local"));
    Assertions.assertSame(StorageType.HDFS, storageType.fromString("hdfs"));
    Assertions.assertSame(ExtendedStorageType.GCS, storageType.fromString("gcs"));
    Assertions.assertSame(ExtendedStorageType.S3, storageType.fromString("s3"));
    Assertions.assertNull(storageType.fromString("non-existing-type"));
  }
}
