package com.linkedin.openhouse.javaclient;

import static com.linkedin.openhouse.javaclient.OpenHouseTableOperations.*;

import com.linkedin.openhouse.client.ssl.HttpConnectionStrategy;
import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke;
import com.linkedin.openhouse.javaclient.builder.ClusteringSpecBuilder;
import com.linkedin.openhouse.javaclient.builder.TimePartitionSpecBuilder;
import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.javaclient.mapper.Privileges;
import com.linkedin.openhouse.javaclient.mapper.SparkMapper;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.UpdateAclPoliciesRequestBody;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Catalog implementation to create, read, update and delete tables in OpenHouse. This class
 * leverages Openhouse tableclient to perform CRUD operations on Tables resource in the Catalog
 * service. This implementation provides client side catalog implementation for Iceberg tables in
 * Java.
 *
 * <p>This is the iceberg-1.5 / Spark-3.5 copy of {@code OpenHouseCatalog}. It extends {@link
 * BaseMetastoreViewCatalog} (instead of {@link BaseMetastoreCatalog}) so a single catalog object
 * serves both tables (inherited, unchanged) and views. This is the first increment of OpenHouse
 * view support: production code, gated and off by default. View operations are active only when
 * {@code spark.sql.catalog.<name>.iceberg-views-enabled=true}, and are backed by an in-memory MOCK
 * store ({@code mockViewStore}) so {@code buildView} -> {@code loadView} round-trips without a
 * persistence service. Evolution: replace {@code mockViewStore} and the inline {@link
 * ViewOperations} in {@link #newViewOps} with a Views-service-backed {@code
 * OpenHouseViewOperations} calling a generated {@code ViewApi}, mirroring how {@link #newTableOps}
 * returns {@code OpenHouseTableOperations} calling {@code TableApi}. The iceberg-1.2 / Spark-3.1
 * copy stays table-only ({@code extends BaseMetastoreCatalog}).
 *
 * <p>Because extending {@link BaseMetastoreViewCatalog} makes this an Iceberg {@code ViewCatalog},
 * Spark's {@code SparkCatalog} routes view probes to this instance instead of short-circuiting
 * them (it only calls a catalog's view methods when the catalog is {@code instanceof ViewCatalog};
 * otherwise it answers view ops itself). Notably {@code SparkCatalog.loadView} is invoked while
 * resolving every unqualified identifier. So when views are disabled we mirror, method-for-method,
 * how {@code SparkCatalog} behaves for a non-{@code ViewCatalog} (table-only) catalog, making the
 * default state indistinguishable from {@code extends BaseMetastoreCatalog}: {@code loadView}
 * throws {@link NoSuchViewException} (so Spark falls back to table resolution rather than
 * hard-failing), {@code listViews} returns empty, and {@code dropView} returns {@code false}, while
 * the create/modify operations {@code buildView} and {@code renameView} throw {@link
 * UnsupportedOperationException}.
 */
@Slf4j
public class OpenHouseCatalog extends BaseMetastoreViewCatalog
    implements Configurable, SupportsNamespaces, SupportsGrantRevoke {

  private TableApi tableApi;

  private ApiClient apiClient;

  private SnapshotApi snapshotApi;

  private DatabaseApi databaseApi;

  private FileIO fileIO;

  private Configuration conf;

  private String cluster;

  private String name;

  protected Map<String, String> properties;

  private static final String DEFAULT_CLUSTER = "local";

  private static final String CLUSTER_PROPERTY = "cluster";

  private static final String AUTH_TOKEN = "auth-token";

  private static final String TRUST_STORE = "trust-store";

  private static final String HTTP_CONNECTION_STRATEGY = "http-connection-strategy";

  public static final String CLIENT_NAME = "client-name";

  public static final String CLIENT_VERSION = "client-version";

  /** Catalog property that gates view support. Off by default. */
  private static final String VIEWS_ENABLED_PROPERTY = "iceberg-views-enabled";

  /** Whether view operations are enabled for this catalog instance (set in {@link #initialize}). */
  private boolean viewsEnabled = false;

  /**
   * In-memory MOCK view store standing in for the OpenHouse Views service until its API and client
   * exist. Holds committed {@link ViewMetadata} by identifier so create/load round-trips work.
   */
  private final ConcurrentHashMap<TableIdentifier, ViewMetadata> mockViewStore =
      new ConcurrentHashMap<>();

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "OpenHouse Table Service URI is required");
    log.info("Establishing connection with OpenHouse service at " + uri);
    String truststore = properties.getOrDefault(TRUST_STORE, "");
    String token = properties.getOrDefault(AUTH_TOKEN, null);
    String httpConnectionStrategy = properties.getOrDefault(HTTP_CONNECTION_STRATEGY, null);
    String clientName = properties.getOrDefault(CLIENT_NAME, null);
    String clientVersion = properties.getOrDefault(CLIENT_VERSION, null);
    try {
      TablesApiClientFactory tablesApiClientFactory = TablesApiClientFactory.getInstance();
      tablesApiClientFactory.setStrategy(HttpConnectionStrategy.fromString(httpConnectionStrategy));
      tablesApiClientFactory.setClientName(clientName);
      tablesApiClientFactory.setClientVersion(clientVersion);
      if (properties.containsKey(CatalogProperties.APP_ID)) {
        tablesApiClientFactory.setSessionId(properties.get(CatalogProperties.APP_ID));
      }
      this.apiClient = tablesApiClientFactory.createApiClient(uri, token, truststore);
    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(
          "OpenHouse Catalog initialization failed: Failure while initializing ApiClient", e);
    }
    this.tableApi = new TableApi(apiClient);
    this.snapshotApi = new SnapshotApi(apiClient);
    this.databaseApi = new DatabaseApi(apiClient);

    this.fileIO = loadFileIO(properties);

    this.cluster = properties.getOrDefault(CLUSTER_PROPERTY, DEFAULT_CLUSTER);
    this.viewsEnabled =
        Boolean.parseBoolean(properties.getOrDefault(VIEWS_ENABLED_PROPERTY, "false"));
    if (viewsEnabled) {
      log.warn(
          "OpenHouse view support is ENABLED (in-memory MOCK backend). Views are not "
              + "persisted to any service and are visible only within this catalog instance.");
    }
  }

  protected FileIO loadFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    return fileIOImpl == null
        ? new HadoopFileIO(this.conf)
        : CatalogUtil.loadFileIO(fileIOImpl, properties, this.conf);
  }

  /**
   * updates the auth token in ApiClient's default header which gets added to every request from
   * ApiClient
   *
   * @param token
   */
  protected void updateAuthToken(String token) {
    if (token != null && !token.isEmpty()) {
      this.properties.put(AUTH_TOKEN, token);
      this.apiClient.addDefaultHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
    }
  }

  /**
   * returns an unmodifiableMap of catalog properties preserving original properties
   *
   * @return
   */
  @Override
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(properties);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    log.info("Calling listTables with namespace: {}", namespace.toString());
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    } else if (namespace.toString().isEmpty()) {
      throw new ValidationException(
          "DatabaseId was not provided, for SQL please run \"SHOW TABLES IN <databaseId>\" instead");
    }
    List<TableIdentifier> tables =
        tableApi
            .searchTablesV1(namespace.toString())
            .map(GetAllTablesResponseBody::getResults)
            .flatMapMany(Flux::fromIterable)
            .map(SparkMapper::toTableIdentifier)
            .collectList()
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .onErrorResume(
                WebClientRequestException.class,
                e -> Mono.error(new WebClientRequestWithMessageException(e)))
            .block();
    log.debug("Calling listTables succeeded");
    return tables;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    log.info(
        "Calling dropTable with identifier: {}, and purge option: {}",
        identifier.toString(),
        purge);
    if (identifier.namespace().levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels "
              + String.join(".", identifier.namespace().levels()));
    }
    // Default to purge = true regardless of the input parameter
    // Currently, SparkCatalog (3.1 and 3.5) will always call dropTable with purge = false and
    // handle purge in purgeTable()
    // To handle on catalog side, we should look to override purgeTable()
    // https://spark.apache.org/docs/3.5.1/api/java/org/apache/spark/sql/connector/catalog/TableCatalog.html#purgeTable(org.apache.spark.sql.connector.catalog.Identifier)

    try {
      tableApi
          .deleteTableV1(identifier.namespace().toString(), identifier.name())
          .onErrorResume(
              WebClientResponseException.NotFound.class,
              e -> Mono.error(new NoSuchTableException("Table " + identifier + " does not exist")))
          .onErrorResume(
              WebClientResponseException.class,
              e -> Mono.error(new WebClientResponseWithMessageException(e)))
          .onErrorResume(
              WebClientRequestException.class,
              e -> Mono.error(new WebClientRequestWithMessageException(e)))
          .block();

    } catch (NoSuchTableException e) {
      log.debug("Table: {} does not exist", identifier.toString());
      return false;
    }
    log.debug("Calling dropTable succeeded");
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    log.info(
        "Calling renameTable from table identifier: {}, to table identifier: {}",
        from.toString(),
        to.toString());

    if (from.namespace().levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels "
              + String.join(".", from.namespace().levels()));
    }

    CatalogAndDbNameFromNamespace catalogAndDbName =
        new CatalogAndDbNameFromNamespace(to.namespace());
    if (catalogAndDbName.catalogName() != null
        && !catalogAndDbName.catalogName().equals(this.name())) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot rename tables across catalogs: from=%s, to=%s",
              String.join(".", this.name(), from.toString()), to));
    }

    tableApi
        .renameTableV1(
            from.namespace().toString(), from.name(), catalogAndDbName.dbName(), to.name())
        .onErrorResume(
            WebClientResponseException.NotFound.class,
            e -> Mono.error(new NoSuchTableException("Table " + from + " does not exist")))
        .onErrorResume(
            WebClientResponseException.class,
            e -> Mono.error(new WebClientResponseWithMessageException(e)))
        .onErrorResume(
            WebClientRequestException.class,
            e -> Mono.error(new WebClientRequestWithMessageException(e)))
        .block();
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return OpenHouseTableOperations.builder()
        .tableIdentifier(tableIdentifier)
        .fileIO(fileIO)
        .tableApi(tableApi)
        .snapshotApi(snapshotApi)
        .cluster(cluster)
        .build();
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier != null && tableIdentifier.namespace().levels().length == 1;
  }

  /**
   * it's necessary to return null. This function only gets called from {@link
   * BaseMetastoreCatalog}, just before doCommit(). {@link
   * OpenHouseTableOperations#doCommit(org.apache.iceberg.TableMetadata,
   * org.apache.iceberg.TableMetadata)} currently ignores the return value of null.
   *
   * <p>Without this return, an error will be thrown for a simple (CREATE TABLE) statement.
   *
   * <p>This behavior cannot be changed for OH tables, it is decided by table service.
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return null;
  }

  /**
   * A {@link BaseMetastoreCatalog} needs to be set as {@link Configurable}.
   *
   * <p>The {@link org.apache.iceberg.spark.SparkCatalog} extensions will provide the right Hadoop
   * configurations from the spark environment when building a custom catalog.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void createNamespace(Namespace namespace) throws UnsupportedOperationException {
    createNamespace(namespace, null);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Create Database is not supported");
  }

  /**
   * List all databases. Support for "show databases" where only the top level databases will be
   * shown.
   *
   * @return
   */
  @Override
  public List<Namespace> listNamespaces() {
    log.info("Calling listNamespaces");
    List<Namespace> namespaces =
        databaseApi
            .getAllDatabasesV1()
            .map(GetAllDatabasesResponseBody::getResults)
            .flatMapMany(Flux::fromIterable)
            .map(SparkMapper::toNamespaces)
            .collectList()
            .block();
    log.debug("Calling listNamespaces succeeded");
    return namespaces;
  }

  /**
   * List databases under a database. Support for "drop database" where the default behavior is
   * cascading and needs to visit databases recursively. We are not supporting multi-level
   * databases, so no need to implement this method.
   *
   * @return
   */
  @Override
  public List<Namespace> listNamespaces(Namespace namespace)
      throws NoSuchNamespaceException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Openhouse supports 2-lvl namespace <schema>.<table>");
  }

  /**
   * Support for "describe database". Implement this if needed.
   *
   * @return
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Describing database is not supported");
  }

  @Override
  public boolean dropNamespace(Namespace namespace)
      throws NamespaceNotEmptyException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Drop database is not supported");
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> map)
      throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Set properties on a database is not supported");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> set)
      throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Remove properties of a database is not supported");
  }

  @Override
  public boolean namespaceExists(Namespace namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Checking if database exists is not supported");
  }

  @Override
  public void updateTableAclPolicies(
      TableIdentifier tableIdentifier, boolean isGrant, String privilege, String principal) {
    log.info(
        "Calling updateTableAclPolicies with identifier: {}, isGrant: {}, privilege: {}, principal: {}",
        tableIdentifier.toString(),
        isGrant,
        privilege,
        principal);
    if (tableIdentifier.namespace().levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels "
              + String.join(".", tableIdentifier.namespace().levels()));
    }
    tableApi
        .updateAclPoliciesV1(
            tableIdentifier.namespace().toString(),
            tableIdentifier.name(),
            getUpdateAclPoliciesRequestBody(
                isGrant, principal, Privileges.fromPrivilege(privilege).getRole()))
        .onErrorResume(
            WebClientResponseException.BadRequest.class,
            e ->
                Mono.error(
                    new IllegalArgumentException(
                        e.getStatusCode().value() + " , " + e.getResponseBodyAsString(), e)))
        .onErrorResume(
            WebClientResponseException.class,
            e -> Mono.error(new WebClientResponseWithMessageException(e)))
        .onErrorResume(
            WebClientRequestException.class,
            e -> Mono.error(new WebClientRequestWithMessageException(e)))
        .block();
    log.debug("Calling updateTableAclPolicies succeeded");
  }

  @Override
  public List<AclPolicyDto> getTableAclPolicies(TableIdentifier tableIdentifier) {
    log.info("Calling getTableAclPolicies with identifier: {}", tableIdentifier.toString());
    if (tableIdentifier.namespace().levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels "
              + String.join(".", tableIdentifier.namespace().levels()));
    }
    List<AclPolicyDto> aclPolicies =
        tableApi.getAclPoliciesV1(tableIdentifier.namespace().toString(), tableIdentifier.name())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .onErrorResume(
                WebClientRequestException.class,
                e -> Mono.error(new WebClientRequestWithMessageException(e)))
            .blockOptional().map(GetAclPoliciesResponseBody::getResults)
            .orElse(Collections.emptyList()).stream()
            .map(SparkMapper::toAclPolicyDto)
            .collect(Collectors.toList());

    log.debug("Calling getTableAclPolicies succeeded");
    return aclPolicies;
  }

  @Override
  public void updateDatabaseAclPolicies(
      Namespace identifier, boolean isGrant, String privilege, String principal) {
    log.info(
        "Calling updateDatabaseAclPolicies with namespace: {}, isGrant: {}, privilege: {}, principal: {}",
        identifier.toString(),
        isGrant,
        privilege,
        principal);
    if (identifier.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", identifier.levels()));
    }
    databaseApi
        .updateDatabaseAclPoliciesV1(
            identifier.toString(),
            getUpdateAclPoliciesRequestBody(
                isGrant, principal, Privileges.fromPrivilege(privilege).getRole()))
        .onErrorResume(
            WebClientResponseException.BadRequest.class,
            e ->
                Mono.error(
                    new IllegalArgumentException(
                        e.getStatusCode().value() + " , " + e.getResponseBodyAsString(), e)))
        .onErrorResume(
            WebClientResponseException.class,
            e -> Mono.error(new WebClientResponseWithMessageException(e)))
        .onErrorResume(
            WebClientRequestException.class,
            e -> Mono.error(new WebClientRequestWithMessageException(e)))
        .block();
    log.debug("Calling updateDatabaseAclPolicies succeeded");
  }

  @Override
  public List<AclPolicyDto> getDatabaseAclPolicies(Namespace namespace) {
    log.info("Calling getDatabaseAclPolicies with identifier: {}", namespace.toString());
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }
    List<AclPolicyDto> aclPolicies =
        databaseApi.getDatabaseAclPoliciesV1(namespace.toString())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .onErrorResume(
                WebClientRequestException.class,
                e -> Mono.error(new WebClientRequestWithMessageException(e)))
            .blockOptional().map(GetAclPoliciesResponseBody::getResults)
            .orElse(Collections.emptyList()).stream()
            .map(SparkMapper::toAclPolicyDto)
            .collect(Collectors.toList());

    log.debug("Calling getDatabaseAclPolicies succeeded");
    return aclPolicies;
  }

  private UpdateAclPoliciesRequestBody getUpdateAclPoliciesRequestBody(
      boolean isGrant, String principal, String role) {
    UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody = new UpdateAclPoliciesRequestBody();
    updateAclPoliciesRequestBody.setOperation(
        isGrant
            ? UpdateAclPoliciesRequestBody.OperationEnum.GRANT
            : UpdateAclPoliciesRequestBody.OperationEnum.REVOKE);
    updateAclPoliciesRequestBody.setPrincipal(principal);
    updateAclPoliciesRequestBody.setRole(role);
    return updateAclPoliciesRequestBody;
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new OpenHouseTableBuilder(identifier, schema);
  }

  // ============================= OpenHouse Views (gated, off by default) =============================
  // Gated by VIEWS_ENABLED_PROPERTY: view operations delegate to an in-memory MOCK
  // backend (mockViewStore). loadView/buildView reuse the BaseMetastoreViewCatalog machinery via
  // newViewOps; listViews/dropView/renameView are backed directly by the store.

  /**
   * Guard for the view rename operation ({@link #renameView}). When views are disabled this throws
   * {@link UnsupportedOperationException}. Reached only when a view already exists (rename resolves
   * the source view first), which cannot happen while views are disabled, so it is effectively a
   * safety net for direct (non-Spark) API callers. The create path ({@link #buildView}) instead
   * throws {@link NoSuchNamespaceException} so {@code SparkCatalog.createView} normalizes it to a
   * Spark {@code AnalysisException}; the read/probe operations ({@link #loadView}, {@link
   * #listViews}, {@link #dropView}) each return their "no such view" result so table flows are
   * unaffected.
   */
  private void requireViewsEnabled() {
    if (!viewsEnabled) {
      throw new UnsupportedOperationException("OpenHouse views are unsupported.");
    }
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new ViewOperations() {
      @Override
      public ViewMetadata current() {
        return mockViewStore.get(identifier);
      }

      @Override
      public ViewMetadata refresh() {
        return mockViewStore.get(identifier);
      }

      @Override
      public void commit(ViewMetadata base, ViewMetadata metadata) {
        log.warn(
            "OpenHouse MOCK view commit for {} (in-memory only, not persisted to any service)",
            identifier);
        mockViewStore.put(identifier, metadata);
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * <p>When views are disabled, throws {@link NoSuchViewException} rather than {@link
   * UnsupportedOperationException}. Spark's {@code SparkCatalog.loadView} probes this method while
   * resolving every unqualified identifier and catches only {@code NoSuchViewException} to fall back
   * to table resolution; any other exception propagates and breaks table reads. Throwing {@code
   * NoSuchViewException} here therefore reproduces the table-only (non-{@code ViewCatalog}) behavior.
   */
  @Override
  public View loadView(TableIdentifier identifier) {
    if (!viewsEnabled) {
      throw new NoSuchViewException("View does not exist: %s", identifier);
    }
    log.info("Calling loadView with identifier: {}", identifier);
    return super.loadView(identifier);
  }

  /**
   * {@inheritDoc}
   *
   * <p>A create operation. When views are disabled this throws Iceberg's {@link
   * NoSuchNamespaceException}. {@code CREATE VIEW} reaches this method through {@code
   * SparkCatalog.createView}, which calls {@code buildView(...).create()} and catches only {@code
   * NoSuchNamespaceException} / {@code AlreadyExistsException} (rethrowing them as Spark {@code
   * AnalysisException}s); any other exception — e.g. {@link UnsupportedOperationException} — would
   * leak as a raw runtime error and break callers that expect an {@code AnalysisException}. Throwing
   * {@code NoSuchNamespaceException} is therefore the signal that normalizes {@code CREATE VIEW}
   * rejection to a Spark {@code AnalysisException}, matching how a table-only catalog (Iceberg 1.2 /
   * Spark 3.1) rejects it. See {@code OpenHouseViewSparkITest}.
   */
  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    if (!viewsEnabled) {
      throw new NoSuchNamespaceException(
          "OpenHouse views are not enabled; cannot create view: %s", identifier);
    }
    log.info("Calling buildView with identifier: {}", identifier);
    // OpenHouse tables have no client-side warehouse location (defaultWarehouseLocation returns
    // null), but Iceberg's ViewMetadata requires a non-null location. Supply a mock default so a
    // bare buildView().create() works; an explicit non-null withLocation(...) overrides it.
    return super.buildView(identifier)
        .withLocation("mock://openhouse/views/" + identifier.toString().replace('.', '/'));
  }

  /**
   * {@inheritDoc}
   *
   * <p>When views are disabled, returns an empty list, matching how {@code SparkCatalog} answers
   * {@code SHOW VIEWS} for a non-{@code ViewCatalog} catalog (no views, rather than an error).
   */
  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    if (!viewsEnabled) {
      return Collections.emptyList();
    }
    log.info("Calling listViews with namespace: {}", namespace.toString());
    return mockViewStore.keySet().stream()
        .filter(identifier -> identifier.namespace().equals(namespace))
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * <p>When views are disabled, returns {@code false} (nothing to drop), matching how {@code
   * SparkCatalog} answers {@code DROP VIEW} for a non-{@code ViewCatalog} catalog; this keeps {@code
   * DROP VIEW ... IF EXISTS} a no-op rather than an error.
   */
  @Override
  public boolean dropView(TableIdentifier identifier) {
    if (!viewsEnabled) {
      return false;
    }
    log.info("Calling dropView with identifier: {}", identifier);
    return mockViewStore.remove(identifier) != null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A modify operation: when views are disabled this throws {@link UnsupportedOperationException}
   * via {@link #requireViewsEnabled()}, matching how {@code SparkCatalog} fails {@code ALTER VIEW
   * ... RENAME} for a non-{@code ViewCatalog} catalog.
   */
  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    requireViewsEnabled();
    log.info("Calling renameView from view identifier: {}, to view identifier: {}", from, to);
    ViewMetadata metadata = mockViewStore.remove(from);
    if (metadata == null) {
      throw new NoSuchViewException("View does not exist: %s", from);
    }
    mockViewStore.put(to, metadata);
  }

  /**
   * {@link OpenHouseTableBuilder} re-uses most of its functionality to {@link
   * BaseMetastoreCatalogTableBuilder}, except for: {@link
   * OpenHouseTableBuilder#createTransaction()} and {@link
   * OpenHouseTableBuilder#createOrReplaceTransaction()}
   *
   * <p>Overridden behavior is only for CTAS statements, which is, OpenHouseService is contacted
   * with stage=true, and its returned metadata is used for further data processing.
   */
  private final class OpenHouseTableBuilder extends BaseMetastoreCatalogTableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;

    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();

    OpenHouseTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      super.withPartitionSpec(newSpec);
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        this.propertiesBuilder.putAll(properties);
      }
      super.withProperties(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      super.withProperty(key, value);
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder != null ? sortOrder : SortOrder.unsorted();
      super.withSortOrder(sortOrder);
      return this;
    }

    /**
     * Start a transaction to create or replace a table. If table does not exist the method will
     * stage create the table. If the table exists, it will stage replace the table. The table will
     * be live and queryable for use only after transaction has been committed.
     */
    @Override
    public Transaction createOrReplaceTransaction() {
      TableOperations ops = newTableOps(this.identifier);
      if (ops.current() == null) {
        return createTransaction();
      } else {
        return replaceTransaction();
      }
    }

    /**
     * Start a transaction to replace an existing table. The method will stage replace the table
     * with schema and partition evolution checks bypassed. The table will be live and queryable for
     * use only after transaction has been committed.
     */
    @Override
    public Transaction replaceTransaction() {
      TableOperations ops = newTableOps(this.identifier);
      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", new Object[] {this.identifier});
      }
      TableMetadata metadata = replaceStagedMetadata(ops);
      return Transactions.replaceTableTransaction(this.identifier.toString(), ops, metadata);
    }

    /**
     * Start a transaction to create a table. If table does not exist the method will stage create
     * the table. The table will be live and queryable for use only after transaction has been
     * committed.
     */
    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(this.identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException(
            "Table already exists: %s", new Object[] {this.identifier});
      } else {
        TableMetadata metadata = createStagedMetadata();
        return Transactions.createTableTransaction(this.identifier.toString(), ops, metadata);
      }
    }

    private TableMetadata createStagedMetadata() {
      CreateUpdateTableRequestBody createUpdateTableRequestBody =
          new CreateUpdateTableRequestBody();
      createUpdateTableRequestBody.setTableId(identifier.name());
      createUpdateTableRequestBody.setDatabaseId(identifier.namespace().toString());
      createUpdateTableRequestBody.setClusterId(cluster);
      createUpdateTableRequestBody.setBaseTableVersion(INITIAL_TABLE_VERSION);
      createUpdateTableRequestBody.setSchema(SchemaParser.toJson(schema, false));
      createUpdateTableRequestBody.setStageCreate(true);
      createUpdateTableRequestBody.setTimePartitioning(
          TimePartitionSpecBuilder.builderFor(schema, spec).build());
      createUpdateTableRequestBody.setClustering(
          ClusteringSpecBuilder.builderFor(schema, spec).build());
      createUpdateTableRequestBody.setTableProperties(propertiesBuilder.build());
      createUpdateTableRequestBody.setSortOrder(SortOrderParser.toJson(sortOrder));
      String tableLocation =
          tableApi
              .createTableV1(identifier.namespace().toString(), createUpdateTableRequestBody)
              .onErrorResume(
                  e ->
                      handleCreateUpdateHttpError(
                          e,
                          createUpdateTableRequestBody.getDatabaseId(),
                          createUpdateTableRequestBody.getTableId()))
              .mapNotNull(GetTableResponseBody::getTableLocation)
              .block();
      return new StaticTableOperations(tableLocation, fileIO).refresh();
    }

    private TableMetadata replaceStagedMetadata(TableOperations ops) {
      CreateUpdateTableRequestBody createUpdateTableRequestBody =
          new CreateUpdateTableRequestBody();
      createUpdateTableRequestBody.setTableId(identifier.name());
      createUpdateTableRequestBody.setDatabaseId(identifier.namespace().toString());
      createUpdateTableRequestBody.setClusterId(cluster);
      createUpdateTableRequestBody.setBaseTableVersion(ops.current().metadataFileLocation());
      createUpdateTableRequestBody.setSchema(SchemaParser.toJson(schema, false));
      createUpdateTableRequestBody.setTimePartitioning(
          TimePartitionSpecBuilder.builderFor(schema, spec).build());
      createUpdateTableRequestBody.setClustering(
          ClusteringSpecBuilder.builderFor(schema, spec).build());
      createUpdateTableRequestBody.setTableProperties(propertiesBuilder.build());
      createUpdateTableRequestBody.setSortOrder(SortOrderParser.toJson(sortOrder));
      createUpdateTableRequestBody.setStageReplace(
          true); // indicate this is a replace table operation

      String tableLocation =
          tableApi
              .createTableV1(identifier.namespace().toString(), createUpdateTableRequestBody)
              .onErrorResume(
                  e ->
                      handleCreateUpdateHttpError(
                          e,
                          createUpdateTableRequestBody.getDatabaseId(),
                          createUpdateTableRequestBody.getTableId()))
              .mapNotNull(GetTableResponseBody::getTableLocation)
              .block();
      return new StaticTableOperations(tableLocation, fileIO).refresh();
    }
  }

  /**
   * In scenarios where catalog name is being lumped together with the namespace as it is not being
   * parsed by the Spark Strategy. Needed in some scenarios to maintain compatibility with Hive DDL
   * while also supporting Iceberg Spark DDL. This class is used as a way to parse the catalog name
   * and dbname from a namespace
   */
  private static class CatalogAndDbNameFromNamespace {
    private final String catalogName;
    private final String dbName;

    public CatalogAndDbNameFromNamespace(Namespace namespace) {
      if (namespace.levels().length > 2) {
        throw new ValidationException(
            "Namespace has unexpected levels " + String.join(".", namespace.levels()));
      } else if (namespace.levels().length == 2) {
        this.catalogName = namespace.level(0);
        this.dbName = namespace.level(1);
      } else {
        this.dbName = namespace.toString();
        this.catalogName = null;
      }
    }

    public String catalogName() {
      return this.catalogName;
    }

    public String dbName() {
      return this.dbName;
    }
  }
}
