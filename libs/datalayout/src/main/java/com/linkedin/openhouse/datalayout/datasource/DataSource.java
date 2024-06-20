package com.linkedin.openhouse.datalayout.datasource;

import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;

/**
 * Interface for a data source that provides a {@link Dataset}. Used to plug-in table file and query
 * statistics to generate data layout strategies.
 */
public interface DataSource<T> extends Supplier<Dataset<T>> {}
