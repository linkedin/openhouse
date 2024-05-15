package com.linkedin.openhouse.housetables.config.db.iceberg;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.nio.file.Paths;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configure Iceberg source for {@link HtsRepository}, by providing iceberg specific beans.
 *
 * <p>Ultimate selection of beans to plug-in chosen db is made in {@link
 * com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration}
 */
@Slf4j
@Configuration
public class IcebergProviderConfiguration {

  private static final String HTS_CATALOG_NAME = "htsCatalog";
  private static final String HTS_DATABASE_NAME = "htsDB";
  private static final String HTS_USER_TBL_NAME = "userTable";
  private static final String HTS_JOB_TBL_NAME = "jobTable";

  @Autowired StorageManager storageManager;

  private Catalog provideHadoopCatalogForHouseTables() {
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(getHadoopConfigurations());
    catalog.initialize(
        HTS_CATALOG_NAME,
        Collections.singletonMap(
            CatalogProperties.WAREHOUSE_LOCATION,
            Paths.get(storageManager.getDefaultStorage().getClient().getRootPrefix()).toString()));
    return catalog;
  }

  @Bean
  @ConditionalOnProperty(value = "cluster.housetables.database.type", havingValue = "ICEBERG")
  public IcebergHtsRepository<UserTableIcebergRow, UserTableIcebergRowPrimaryKey>
      provideIcebergHtsRepositoryForUserTable() {
    Catalog catalog = provideHadoopCatalogForHouseTables();
    return IcebergHtsRepository.<UserTableIcebergRow, UserTableIcebergRowPrimaryKey>builder()
        .catalog(catalog)
        .htsTableIdentifier(TableIdentifier.of(HTS_DATABASE_NAME, HTS_USER_TBL_NAME))
        .build();
  }

  @Bean
  @ConditionalOnProperty(value = "cluster.housetables.database.type", havingValue = "ICEBERG")
  public IcebergHtsRepository<JobIcebergRow, JobIcebergRowPrimaryKey>
      provideIcebergHtsRepositoryForJobTable() {
    Catalog catalog = provideHadoopCatalogForHouseTables();
    return IcebergHtsRepository.<JobIcebergRow, JobIcebergRowPrimaryKey>builder()
        .catalog(catalog)
        .htsTableIdentifier(TableIdentifier.of(HTS_DATABASE_NAME, HTS_JOB_TBL_NAME))
        .build();
  }

  private org.apache.hadoop.conf.Configuration getHadoopConfigurations() {
    log.debug("Loading hadoop configuration for:" + storageManager.getDefaultStorage().getType());
    if (storageManager.getDefaultStorage().getType().equals(StorageType.HDFS)
        || storageManager.getDefaultStorage().getType().equals(StorageType.LOCAL)) {
      return ((FileSystem) storageManager.getDefaultStorage().getClient().getNativeClient())
          .getConf();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported storage type for Iceberg catalog: "
              + storageManager.getDefaultStorage().getType());
    }
  }
}
