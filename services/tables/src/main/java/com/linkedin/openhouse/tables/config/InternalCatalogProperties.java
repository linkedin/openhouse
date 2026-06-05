package com.linkedin.openhouse.tables.config;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@Getter
@Setter
@ConfigurationProperties(prefix = "cluster.iceberg.tables")
public class InternalCatalogProperties {

  private MetadataCache metadataCache = new MetadataCache();

  private Audit audit = new Audit();

  @Getter
  @Setter
  public static class MetadataCache {
    private Boolean enabled;
    private Duration ttl;
    private DataSize maxWeight;
  }

  @Getter
  @Setter
  public static class Audit {
    /**
     * Allowlist of property-key regular expressions. A committed table property is included on the
     * TableAuditEvent if its key fully matches at least one pattern (Java regex semantics, {@link
     * java.util.regex.Pattern#matches}). Default empty = nothing emitted.
     */
    private List<String> tablePropertiesAllowlist = Collections.emptyList();

    /**
     * Maximum UTF-8 byte size of a single audited property value. Values larger than this are
     * skipped.
     */
    private DataSize tablePropertyValueMaxSize = DataSize.ofKilobytes(256);

    /**
     * Maximum combined UTF-8 byte size of all audited property values. Properties are visited in
     * sorted key order; a property whose value would push the running total past this cap is
     * skipped, but smaller later values that still fit are kept.
     */
    private DataSize tablePropertiesTotalMaxSize = DataSize.ofKilobytes(512);
  }
}
