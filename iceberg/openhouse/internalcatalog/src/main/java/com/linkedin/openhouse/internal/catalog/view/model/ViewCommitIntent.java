package com.linkedin.openhouse.internal.catalog.view.model;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;

/** Version-neutral inputs. UUID, root, and storage are required on CREATE, ignored on REPLACE. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewCommitIntent {

  private final String databaseId;

  private final String viewId;

  private final Schema schema;

  private final List<SqlViewRepresentationIntent> representations;

  private final String sourceDialect;

  private final String defaultCatalog;

  /** Null is normalized to {@link Namespace#empty()}. */
  private final Namespace defaultNamespace;

  private final Map<String, String> viewProperties;

  /** Required: true for CREATE, false for REPLACE; null is rejected at commit. */
  private final Boolean isCreate;

  /** Trusted HTS snapshot; null means observed absence. Its tableLocation is the REPLACE token. */
  private final HouseTable baseRow;

  private final String creator;

  private final String viewUuid;

  private final String viewLocation;

  private final String storageType;
}
