package org.apache.spark.sql.delta.storage;

import com.linkedin.openhouse.delta.OHCatalog;
import com.linkedin.openhouse.tables.client.api.DeltaSnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.DeltaActionsRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.CommitInfo;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.Iterator;

public class OHLogStore extends HDFSLogStore {

  TableApi _tableApi;
  DeltaSnapshotApi _deltaSnapshotApi;

  public static final ConcurrentHashMap<Identifier, GetTableResponseBody> TABLE_CACHE =
      new ConcurrentHashMap<>();

  public OHLogStore(SparkConf sparkConf, Configuration defaultHadoopConf) {
    super(sparkConf, defaultHadoopConf);
    String prefix = "spark.sql.catalog.openhouse.";
    Map<String, String> stringStringMap = new HashMap();
    Arrays.stream(sparkConf.getAllWithPrefix(prefix)).forEach(x -> stringStringMap.put(x._1, x._2));
    OHCatalog ohCatalog = new OHCatalog();
    ohCatalog.initialize("openhouse", new CaseInsensitiveStringMap(stringStringMap));
    _tableApi = ohCatalog.tableApi;
    _deltaSnapshotApi = ohCatalog.deltaSnapshotApi;
  }

  @Override
  public void write(Path path, Iterator<String> actions, boolean overwrite) {
    // Hack to get tableMetadata
    // solving it the right way can be done via
    // TableApi.searchApi(location=path.toString()) and then get the dbId and tableId
    GetTableResponseBody lastLoadedTable =
        TABLE_CACHE.entrySet().stream()
            .filter(x -> path.toString().contains(x.getValue().getTableLocation()))
            .collect(Collectors.toList())
            .get(0)
            .getValue();

    DeltaActionsRequestBody deltaActionsRequestBody = new DeltaActionsRequestBody();

    List<String> actionsList = new ArrayList<>();
    actions.toStream().foreach(x -> actionsList.add(x));
    deltaActionsRequestBody.setJsonActions(actionsList);

    Integer baseVersion =
        ((Long) ((CommitInfo) Action.fromJson(actionsList.get(0))).readVersion().get()).intValue();

    deltaActionsRequestBody.setBaseTableVersion(baseVersion.toString());
    _deltaSnapshotApi
        .patchDeltaActionsV1(
            lastLoadedTable.getDatabaseId(), lastLoadedTable.getTableId(), deltaActionsRequestBody)
        .block();
  }
}
