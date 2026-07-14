package com.linkedin.openhouse.spark.catalogtest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A local filesystem that actually records and surfaces the replication factor requested at file
 * creation time, so tests can assert on it end-to-end.
 *
 * <p>Hadoop's stock local filesystem support silently discards any requested replication factor in
 * two independent places: {@link LocalFileSystem} (a {@code ChecksumFileSystem}) only forwards the
 * {@code replication} argument of {@code create(...)} down to the underlying raw filesystem when
 * checksums are disabled — by default it wraps the write in a checksum stream that never looks at
 * the argument at all — and {@link RawLocalFileSystem} itself accepts a {@code replication}
 * parameter on {@code create(...)} but never stores or uses it, since a local disk has no concept
 * of block replication.
 *
 * <p>This class disables checksums (so the requested replication factor reaches the raw filesystem)
 * and tracks the replication factor requested per-path in memory, so it can be read back later via
 * {@link #getRequestedReplication(Path)} or observed through the standard {@link
 * #getFileStatus(Path)} API. It is intended for test verification only.
 */
public class ReplicationTrackingLocalFileSystem extends LocalFileSystem {

  public ReplicationTrackingLocalFileSystem() {
    super(new TrackingRawLocalFileSystem());
    // ChecksumFileSystem#create only forwards the requested replication factor to the
    // underlying raw filesystem when checksum writing is disabled; otherwise it opens the raw
    // file through a create() overload that drops the replication argument entirely.
    setWriteChecksum(false);
    setVerifyChecksum(false);
  }

  /**
   * @param path path of a file previously created through this filesystem
   * @return the replication factor that was requested when {@code path} was created, or this
   *     filesystem's default replication if no replication was explicitly requested for it
   */
  public short getRequestedReplication(Path path) {
    return ((TrackingRawLocalFileSystem) getRawFileSystem()).getTrackedReplication(path);
  }

  private static class TrackingRawLocalFileSystem extends RawLocalFileSystem {
    // fs.file.impl.disable.cache is required (see ReplicationTrackingLocalFileSystem's javadoc)
    // to guarantee this class is actually instantiated instead of a previously-cached stock
    // LocalFileSystem, but that means FileSystem.get(...) creates a brand-new instance on every
    // call. The tracking map must therefore be static so that the instance used by Iceberg's
    // write path and the instance used later for test verification share the same data.
    private static final Map<Path, Short> replicationByPath = new ConcurrentHashMap<>();

    short getTrackedReplication(Path path) {
      Short replication = replicationByPath.get(makeQualified(path));
      return replication == null ? getDefaultReplication(path) : replication;
    }

    @Override
    public FSDataOutputStream create(
        Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
        throws IOException {
      track(f, replication);
      return super.create(f, overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      track(f, replication);
      return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      track(f, replication);
      return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean setReplication(Path src, short replication) throws IOException {
      track(src, replication);
      return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      FileStatus status = super.getFileStatus(f);
      Short replication = replicationByPath.get(makeQualified(f));
      if (replication == null || replication <= 0) {
        return status;
      }
      return new FileStatus(
          status.getLen(),
          status.isDirectory(),
          replication,
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          status.getPermission(),
          status.getOwner(),
          status.getGroup(),
          status.getPath());
    }

    private void track(Path f, short replication) {
      if (replication > 0) {
        replicationByPath.put(makeQualified(f), replication);
      }
    }
  }
}
