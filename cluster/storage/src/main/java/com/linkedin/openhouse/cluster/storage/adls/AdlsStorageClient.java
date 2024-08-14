package com.linkedin.openhouse.cluster.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.linkedin.openhouse.cluster.storage.BaseStorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * AdlsStorageClient is an implementation of the StorageClient interface for Azure Data Lake
 * Storage. It uses the {@link DataLakeFileClient} to interact with Data Lake Storage.
 */
@Slf4j
@Lazy
@Component
public class AdlsStorageClient extends BaseStorageClient<DataLakeFileClient> {

  private static final StorageType.Type ADLS_TYPE = StorageType.ADLS;

  @Autowired private StorageProperties storageProperties;

  private Map properties;

  // TODO: Instantiate DLFC: https://github.com/linkedin/openhouse/issues/148
  private DataLakeFileClient dataLakeClient = null;

  /** Intialize the ADLS Client when the bean is accessed the first time. */
  @PostConstruct
  public synchronized void init() {

    validateProperties();

    // Gets the parameters from the ADLS storage type
    properties =
        new HashMap(storageProperties.getTypes().get(ADLS_TYPE.getValue()).getParameters());

    // Try to create a URI with the endpoint and rootpath
    URI uri;
    try {
      uri = new URI(getEndpoint() + getRootPrefix());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Bad storage properties provided: [endpoint: %s, rootPrefix: %s] for type [%s]",
              getEndpoint(), getRootPrefix(), ADLS_TYPE.getValue()),
          e);
    }
  }

  @Override
  public DataLakeFileClient getNativeClient() {
    return dataLakeClient;
  }

  @Override
  public StorageType.Type getStorageType() {
    return ADLS_TYPE;
  }
}
