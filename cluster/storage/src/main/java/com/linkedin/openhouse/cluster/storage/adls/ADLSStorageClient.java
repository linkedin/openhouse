package com.linkedin.openhouse.cluster.storage.abs;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * ABSStorageClient is an implementation of the StorageClient interface for Azure Blob Storage. It
 * uses the {@link BlobClient} to interact with Blob Storage.
 */
@Slf4j
@Lazy
@Component
public class ADLSStorageClient implements StorageClient<DataLakeFileClient> {

  private static final StorageType.Type ADLS_TYPE = StorageType.ADLS;

  @Autowired private StorageProperties storageProperties;

  private DataLakeFileClient dataLake;

  private static final String DEFAULT_ENDPOINT = "abfs:/";

  private static final String DEFAULT_ROOTPATH = "/tmp/";

  private String endpoint;

  private String rootPath;

  /** Intialize the ADLS Client when the bean is accessed the first time. */
  @PostConstruct
  public synchronized void init() {

    URI uri;
    if (storageProperties.getTypes() != null && !storageProperties.getTypes().isEmpty()) {

      // fail if properties are invalid
      Preconditions.checkArgument(
          storageProperties.getTypes().containsKey(ADLS_TYPE.getValue()),
          "Storage properties doesn't contain type: " + ADLS_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties.getTypes().get(ADLS_TYPE.getValue()).getEndpoint() != null,
          "Storage properties doesn't contain endpoint for: " + ADLS_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties.getTypes().get(ADLS_TYPE.getValue()).getRootPath() != null,
          "Storage properties doesn't contain rootpath for: " + ADLS_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties
              .getTypes()
              .get(ADLS_TYPE.getValue())
              .getEndpoint()
              .startsWith(DEFAULT_ENDPOINT),
          "Storage properties endpoint was misconfigured for: " + ADLS_TYPE.getValue());

      // gets the parameters from the ADLS storage type
      Map properties =
          new HashMap(storageProperties.getTypes().get(ADLS_TYPE.getValue()).getParameters);

      endpoint = storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getEndpoint();
      rootPath = storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getRootPath();
    } else {
      endpoint = DEFAULT_ENDPOINT;
      rootPath = DEFAULT_ROOTPATH;
    }

    // try to create a URI with the endpoint and rootpath
    try {
      uri = new URI(endpoint + rootPath);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Storage properties 'endpoint', 'rootpath' was incorrectly configured for: "
              + ADLS_TYPE.getValue(),
          e);
    }

    // if client uninstantiated, create client
    if (dataLake == null) {
      ADLSFileIO fileIO = new ADLSFileIO();
      fileIO.initialize(properties);

      this.dataLake = fileIO.fileClient(uri);
    }
  }

  @Override
  public DataLakeFileClient getNativeClient() {
    return dataLake;
  }

  @Override
  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String getRootPrefix() {
    return rootPath;
  }
}
