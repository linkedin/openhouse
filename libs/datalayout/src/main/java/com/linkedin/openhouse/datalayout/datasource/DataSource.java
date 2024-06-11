package com.linkedin.openhouse.datalayout.datasource;

import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;

public interface DataSource<T> extends Supplier<Dataset<T>> {}
