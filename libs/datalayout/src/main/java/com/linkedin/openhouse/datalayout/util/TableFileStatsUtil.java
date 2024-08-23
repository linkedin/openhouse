package com.linkedin.openhouse.datalayout.util;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

public class TableFileStatsUtil {

  public static Tuple2<Long, Integer> getAggregatedFileStats(
      TableFileStats tableFileStats, long targetBytesSize) {
    return tableFileStats
        .get()
        .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
        .filter(
            (FilterFunction<Long>)
                size -> size < targetBytesSize * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT)
        .map(
            (MapFunction<Long, Tuple2<Long, Integer>>) size -> new Tuple2<>(size, 1),
            Encoders.tuple(Encoders.LONG(), Encoders.INT()))
        .reduce(
            (ReduceFunction<Tuple2<Long, Integer>>)
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
  }
}
