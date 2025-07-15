package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public final class HouseTableModelConstants {

  private HouseTableModelConstants() {}

  private static final Map<String, String> TABLE_PROPS =
      new HashMap<String, String>() {
        {
          put("user.a", "b");
          put("user.c", "d");
        }
      };

  public static final HouseTable HOUSE_TABLE =
      HouseTable.builder()
          .tableId("t1")
          .databaseId("d1")
          .clusterId("c1")
          .tableUri("c1.d1.t1")
          .tableLocation("loc1")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableUUID(UUID.randomUUID().toString())
          .storageType("local")
          .build();

  public static final HouseTable HOUSE_TABLE_SAME_DB =
      HouseTable.builder()
          .tableId("t2")
          .databaseId("d1")
          .clusterId("c1")
          .tableUri("c1.d1.t2")
          .tableLocation("loc2")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableUUID(UUID.randomUUID().toString())
          .storageType("local")
          .build();

  public static final HouseTable HOUSE_TABLE_DIFF_DB =
      HouseTable.builder()
          .tableId("t1")
          .databaseId("d2")
          .clusterId("c1")
          .tableUri("c1.d1.t2")
          .tableLocation("loc3")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableUUID(UUID.randomUUID().toString())
          .storageType("local")
          .build();

  public static HouseTable buildHouseTableWithDbTbl(String db, String table) {
    return HouseTable.builder()
        .tableId(table)
        .databaseId(db)
        .clusterId("c1")
        .tableUri(String.format("c1.%s.%s", db, table))
        .tableLocation("loc3")
        .tableVersion(String.valueOf(new Random().nextLong()))
        .tableUUID(UUID.randomUUID().toString())
        .storageType("local")
        .build();
  }
}
