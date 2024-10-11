package com.linkedin.openhouse.cluster.storage.selector.impl;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.selector.BaseStorageSelector;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * An implementation of {@link StorageSelector} that takes a regex and storage-type provided in the
 * storage selector params in the yaml configuration and returns storage that's provided if the db
 * and table name matches the regex with db.name matcher, else returns the cluster storage default
 *
 * <pre>
 * Example:
 * storages:
 *     default-type: "local"
 *     storage-selector:
 *       name: "RegexStorageSelector"
 *       parameters:
 *         regex: "regex_pattern"
 *         storage-type: "hdfs"
 * </pre>
 */
@Component
@Slf4j
public class RegexStorageSelector extends BaseStorageSelector {

  private static final String REGEX_CONFIG = "regex";
  private static final String STORAGE_TYPE_CONFIG = "storage-type";

  @Autowired StorageManager storageManager;

  @Autowired StorageProperties storageProperties;

  @Autowired StorageType storageType;
  private Pattern pattern;
  private String providedStorage;

  @PostConstruct
  public void init() {
    if (storageProperties.getStorageSelector() != null
        && RegexStorageSelector.class
            .getSimpleName()
            .equals(storageProperties.getStorageSelector().getName())) {
      log.info("Initializing {} ", this.getName());
      String regex = storageProperties.getStorageSelector().getParameters().get(REGEX_CONFIG);
      Preconditions.checkNotNull(
          regex,
          String.format("%s pattern not defined in %s parameters", REGEX_CONFIG, this.getName()));
      pattern = Pattern.compile(regex);
      providedStorage =
          storageProperties.getStorageSelector().getParameters().get(STORAGE_TYPE_CONFIG);
      Preconditions.checkNotNull(
          providedStorage,
          String.format("%s not defined in %s parameters", STORAGE_TYPE_CONFIG, this.getName()));
    }
  }

  /**
   * Returns provided storage if db and table match regex pattern returns cluster storage default
   * otherwise
   *
   * @param db
   * @param table
   * @return Storage
   */
  @Override
  public Storage selectStorage(String db, String table) {
    Matcher matcher = pattern.matcher(db + "." + table);
    if (matcher.matches()) {
      log.info("Selected storage={} for {}.{}", providedStorage, db, table);
      return storageManager.getStorage(storageType.fromString(providedStorage));
    }

    log.info(
        "{}.{} do not match supplied regex pattern {}, Using cluster storage default {}",
        db,
        table,
        pattern.pattern(),
        storageManager.getDefaultStorage().getType().getValue());
    return storageManager.getDefaultStorage();
  }
}
