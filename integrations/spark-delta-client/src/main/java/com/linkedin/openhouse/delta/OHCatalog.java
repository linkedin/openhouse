package com.linkedin.openhouse.delta;

import com.linkedin.openhouse.client.ssl.HttpConnectionStrategy;
import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.tables.client.api.DeltaSnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLException;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.storage.OHLogStore;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import scala.Option;

public class OHCatalog implements TableCatalog {
  private static final String INITIAL_TABLE_VERSION = "-1";
  private static final String AUTH_TOKEN = "auth-token";
  private static final String TRUST_STORE = "trust-store";
  private static final String HTTP_CONNECTION_STRATEGY = "http-connection-strategy";
  public TableApi tableApi;

  public DeltaSnapshotApi deltaSnapshotApi;
  private String clusterName;

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    if (namespace.length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace));
    } else if (namespace.length == 0) {
      throw new ValidationException(
          "DatabaseId was not provided, for SQL please run \"SHOW TABLES IN <databaseId>\" instead");
    }
    List<Identifier> tables =
        tableApi
            .searchTablesV1(namespace[0])
            .map(GetAllTablesResponseBody::getResults)
            .flatMapMany(Flux::fromIterable)
            .map(
                x -> {
                  assert x.getTableId() != null;
                  return Identifier.of(new String[] {x.getDatabaseId()}, x.getTableId());
                })
            .collectList()
            .block();
    assert tables != null;
    return tables.toArray(new Identifier[0]);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Optional<GetTableResponseBody> a =
        tableApi
            .getTableV1(ident.namespace()[0], ident.name())
            .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
            .blockOptional();
    if (!a.isPresent()) {
      throw new NoSuchTableException(ident);
    }

    assert a.get().getTableLocation() != null;
    assert a.get().getTableProperties() != null;

    OHLogStore.TABLE_CACHE.put(ident, a.get());
    return new DeltaTableV2(
        SparkSession.active(),
        new Path(a.get().getTableLocation()),
        Option.empty(),
        Option.apply(ident.toString()),
        Option.empty(),
        new CaseInsensitiveStringMap(a.get().getTableProperties()));
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    CreateUpdateTableRequestBody createUpdateTableRequestBody = new CreateUpdateTableRequestBody();
    createUpdateTableRequestBody.setTableId(ident.name());
    createUpdateTableRequestBody.setDatabaseId(ident.namespace()[0]);
    createUpdateTableRequestBody.setClusterId(clusterName);
    createUpdateTableRequestBody.setBaseTableVersion(INITIAL_TABLE_VERSION);
    createUpdateTableRequestBody.setSchema(schema.json());
    createUpdateTableRequestBody.setTableProperties(properties);
    GetTableResponseBody a =
        tableApi.createTableV1(ident.namespace()[0], createUpdateTableRequestBody).block();
    assert a != null;
    assert a.getTableLocation() != null;
    assert a.getTableProperties() != null;
    return new DeltaTableV2(
        SparkSession.active(),
        new Path(a.getTableLocation()),
        Option.empty(),
        Option.apply(ident.toString()),
        Option.empty(),
        new CaseInsensitiveStringMap(a.getTableProperties()));
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    if (!tableExists(ident)) {
      return false;
    }
    tableApi.deleteTableV1(ident.namespace()[0], ident.name()).block();
    return true;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {}

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    try {
      String uri = options.get(CatalogProperties.URI);
      Preconditions.checkNotNull(uri, "OpenHouse Table Service URI is required");
      String truststore = options.getOrDefault(TRUST_STORE, "");
      String token = options.getOrDefault(AUTH_TOKEN, null);
      String httpConnectionStrategy = options.getOrDefault(HTTP_CONNECTION_STRATEGY, null);
      clusterName = options.get("cluster");
      ApiClient apiClient = null;
      try {
        TablesApiClientFactory tablesApiClientFactory = TablesApiClientFactory.getInstance();
        tablesApiClientFactory.setStrategy(
            HttpConnectionStrategy.fromString(httpConnectionStrategy));
        apiClient = TablesApiClientFactory.getInstance().createApiClient(uri, token, truststore);
      } catch (MalformedURLException | SSLException e) {
        throw new RuntimeException(
            "OpenHouse Catalog initialization failed: Failure while initializing ApiClient", e);
      }
      tableApi = new TableApi(apiClient);
      deltaSnapshotApi = new DeltaSnapshotApi(apiClient);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public String name() {
    return "openhouse";
  }
}
