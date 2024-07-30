package com.linkedin.openhouse.javaclient;

import static com.linkedin.openhouse.javaclient.OpenHouseTableOperations.*;

import com.linkedin.openhouse.client.ssl.HttpConnectionStrategy;
import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke;
import com.linkedin.openhouse.javaclient.builder.ClusteringSpecBuilder;
import com.linkedin.openhouse.javaclient.builder.TimePartitionSpecBuilder;
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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Catalog implementation to create, read, update and delete tables in OpenHouse. This class
 * leverages Openhouse tableclient to perform CRUD operations on Tables resource in the Catalog
 * service. This implementation provides client side catalog implementation for Iceberg tables in
 * Java.
 */
@Slf4j
public class OpenHouseCatalog extends BaseMetastoreCatalog
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

  private static final String CLIENT_NAME = "client-name";

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
    try {
      TablesApiClientFactory tablesApiClientFactory = TablesApiClientFactory.getInstance();
      tablesApiClientFactory.setStrategy(HttpConnectionStrategy.fromString(httpConnectionStrategy));
      tablesApiClientFactory.setClientName(clientName);
      this.apiClient = tablesApiClientFactory.createApiClient(uri, token, truststore);
      if (properties.containsKey(CatalogProperties.APP_ID)) {
        tablesApiClientFactory.setSessionId(properties.get(CatalogProperties.APP_ID));
      }
    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(
          "OpenHouse Catalog initialization failed: Failure while initializing ApiClient", e);
    }
    this.tableApi = new TableApi(apiClient);
    this.snapshotApi = new SnapshotApi(apiClient);
    this.databaseApi = new DatabaseApi(apiClient);

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        fileIOImpl == null
            ? new HadoopFileIO(this.conf)
            : CatalogUtil.loadFileIO(fileIOImpl, properties, this.conf);

    this.cluster = properties.getOrDefault(CLUSTER_PROPERTY, DEFAULT_CLUSTER);
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
    try {
      tableApi
          .deleteTableV1(identifier.namespace().toString(), identifier.name())
          .onErrorResume(
              WebClientResponseException.NotFound.class,
              e -> {
                throw new NoSuchTableException("Table " + identifier + " does not exist");
              })
          .onErrorResume(
              WebClientResponseException.class,
              e -> {
                throw new WebClientResponseWithMessageException(e);
              })
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
    throw new UnsupportedOperationException("Renaming tables is not supported");
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
    public Transaction createOrReplaceTransaction() {
      throw new UnsupportedOperationException(
          "Replace table is not supported for OpenHouse tables");
    }

    @Override
    public Transaction replaceTransaction() {
      throw new UnsupportedOperationException(
          "Replace table is not supported for OpenHouse tables");
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
}
