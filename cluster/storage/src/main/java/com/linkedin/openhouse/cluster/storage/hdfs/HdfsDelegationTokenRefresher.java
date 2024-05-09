package com.linkedin.openhouse.cluster.storage.hdfs;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * This class is responsible for updating UserGroupInformation {@link
 * org.apache.hadoop.security.UserGroupInformation} with updated hadoop credentials at a regular
 * scheduled interval. This class assumes that hadoop delegation tokens are renewed on a regular
 * basis and token file as pointed by HADOOP_TOKEN_FILE_LOCATION is always updated.
 */
@Slf4j
public class HdfsDelegationTokenRefresher {

  @Autowired HdfsStorage hdfsStorage;

  /**
   * Schedule credential refresh (hadoop delegation tokens) daily twice. The schedule cron
   * expression represented by HdfsStorage specific parameter "token.refresh.schedule.cron" sets the
   * cron to run every 12 hours i.e. daily twice. Hadoop delegation token is valid for 24 hours and
   * hence the token must be refreshed before that. The hadoop delegation token file is pointed by
   * environment variable i.e. HADOOP_TOKEN_FILE_LOCATION. The renewal of the delegation token must
   * be done before it expires. This code assumes that hadoop delegation tokens are renewed on a
   * regular basis and token file as pointed by HADOOP_TOKEN_FILE_LOCATION is always updated. So,
   * this methods reads the token file and updates the current user UserGroupInformation (UGI) with
   * the renewed token and this update is done daily twice. The relevant configuration in the
   * cluster YAML file is as follows:
   *
   * <pre>
   * cluster:
   *   storages:
   *     hdfs:
   *       parameter:
   *         token.refresh.enabled: true
   *         token.refresh.schedule.cron: 0 0 0/12 * * ?
   * </pre>
   */
  @Scheduled(
      cron =
          "#{hdfsStorage.getProperties().getOrDefault('token.refresh.schedule.cron', '0 0 0/12 * * ?')}")
  public void refresh() {
    log.info("Refreshing HDFS delegation token");
    String tokenFileLocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
    try {
      log.info(
          "UserGroupInformation current username :: {} ",
          UserGroupInformation.getCurrentUser().getUserName());
      if (tokenFileLocation != null) {
        File tokenFile = new File(tokenFileLocation);
        log.info(
            "Reading credentials from location set in {}: {}",
            HADOOP_TOKEN_FILE_LOCATION,
            tokenFile.getCanonicalPath());
        if (!tokenFile.isFile()) {
          throw new FileNotFoundException(
              "Source file "
                  + tokenFile.getCanonicalPath()
                  + " from "
                  + HADOOP_TOKEN_FILE_LOCATION
                  + " not found");
        }
        FileSystem fs = (FileSystem) hdfsStorage.getClient().getNativeClient();
        Credentials cred = Credentials.readTokenStorageFile(tokenFile, fs.getConf());
        log.info("Loaded {} tokens", cred.numberOfTokens());
        UserGroupInformation.getCurrentUser().addCredentials(cred);
        log.info(
            "Updated UserGroupInformation current user {} credentials",
            UserGroupInformation.getCurrentUser());
      } else {
        throw new RuntimeException(
            "Unable to load token as HADOOP_TOKEN_FILE_LOCATION env variable is not set");
      }
    } catch (IOException ex) {
      log.error("Exception while refreshing credentials", ex);
      throw new UncheckedIOException(ex);
    }
  }
}
