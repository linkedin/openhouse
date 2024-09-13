package com.linkedin.openhouse.cluster.storage.selector.impl;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.selector.BaseStorageSelector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StorageInNameRegexSelector extends BaseStorageSelector {

  private static final String REGEX_CONFIG = "regex";
  private static final String STORAGE_TYPE_CONFIG = "storage-type";

  @Autowired private StorageManager storageManager;

  @Autowired private StorageProperties storageProperties;

  @Autowired private StorageType storageType;
  private Pattern pattern;
  private String providedStorage;

  @PostConstruct
  public void init() {
    log.info("Initializing {} ", this.getName());
    String regex = storageProperties.getStorageSelector().getParameters().get(REGEX_CONFIG);
    Preconditions.checkNotNull(regex, "Regex pattern for db and table cannot be null");
    pattern = Pattern.compile(regex);
    providedStorage =
        storageProperties.getStorageSelector().getParameters().get(STORAGE_TYPE_CONFIG);
    Preconditions.checkNotNull(
        providedStorage, "{} not defined in {} parameters", STORAGE_TYPE_CONFIG, this.getName());
  }

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
