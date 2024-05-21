package com.linkedin.openhouse.javaclient.fileIO;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class S3HardcodedPrefixFileIO extends org.apache.iceberg.aws.s3.S3FileIO {

  public static final String PREFIX = "s3:/";

  @Override
  public InputFile newInputFile(String path) {
    return new DelegatingInputFile(super.newInputFile(appendIfNotPresent(path)));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new DelegatingInputFile(super.newInputFile(appendIfNotPresent(path), length));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new DelegatingOutputFile(super.newOutputFile(appendIfNotPresent(path)));
  }

  @Override
  public void deleteFile(String path) {
    super.deleteFile(appendIfNotPresent(path));
  }

  String appendIfNotPresent(String path) {
    if (path.startsWith(PREFIX)) {
      return path;
    }
    return PREFIX + path;
  }

  @AllArgsConstructor
  public static class DelegatingInputFile implements InputFile {
    @Delegate InputFile delegate;

    @Override
    public String location() {
      if (delegate.location().startsWith(PREFIX)) {
        return delegate.location().substring(4);
      } else {
        return delegate.location();
      }
    }
  }

  @AllArgsConstructor
  public static class DelegatingOutputFile implements OutputFile {
    @Delegate OutputFile delegate;

    @Override
    public String location() {
      if (delegate.location().startsWith(PREFIX)) {
        return delegate.location().substring(4);
      } else {
        return delegate.location();
      }
    }

    @Override
    public InputFile toInputFile() {
      return new DelegatingInputFile(delegate.toInputFile());
    }
  }
}
