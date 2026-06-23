package com.linkedin.openhouse.jobs.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Builds a SQL {@code WHERE} clause that selects a set of partitions, given the partition column
 * names and a list of partition-value tuples.
 *
 * <p>For partition columns {@code [date, hour, late]} and value tuples {@code [[a, b, c], [a, b,
 * d]]} the produced clause is:
 *
 * <pre>
 *   (`date` = 'a' AND `hour` = 'b' AND `late` = 'c') OR (`date` = 'a' AND `hour` = 'b' AND `late` = 'd')
 * </pre>
 *
 * <p>Values equal to the literal {@code "null"} (the string emitted by the strategy generator for a
 * null partition value) are rendered as {@code `col` IS NULL}. Other values are rendered as
 * single-quoted string literals; Spark implicitly casts them to the column type when the clause is
 * resolved against the table schema (handles string/int/date partition columns).
 */
public final class PartitionWhereClauseBuilder {
  private static final String NULL_LITERAL = "null";

  private PartitionWhereClauseBuilder() {}

  /**
   * @param partitionColumns comma-separated partition column names, e.g. {@code "date,hour,late"}
   *     (surrounding whitespace per token is tolerated, matching the generator's {@code ", "} join)
   * @param partitionValueTuples one entry per partition, each a comma-separated value tuple aligned
   *     with {@code partitionColumns}, e.g. {@code "a,b,c"}
   * @return the {@code WHERE} clause body (without the leading {@code WHERE})
   */
  public static String build(String partitionColumns, List<String> partitionValueTuples) {
    List<String> columns = split(partitionColumns);
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("partitionColumns must not be empty");
    }
    if (partitionValueTuples == null || partitionValueTuples.isEmpty()) {
      throw new IllegalArgumentException("partitionValueTuples must not be empty");
    }
    List<String> orTerms = new ArrayList<>();
    for (String tuple : partitionValueTuples) {
      List<String> values = split(tuple);
      if (values.size() != columns.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Partition value tuple [%s] has %d values but there are %d partition columns %s",
                tuple, values.size(), columns.size(), columns));
      }
      List<String> andTerms = new ArrayList<>();
      for (int i = 0; i < columns.size(); i++) {
        andTerms.add(predicate(columns.get(i), values.get(i)));
      }
      orTerms.add("(" + String.join(" AND ", andTerms) + ")");
    }
    return String.join(" OR ", orTerms);
  }

  private static String predicate(String column, String value) {
    String quotedColumn = "`" + column + "`";
    if (NULL_LITERAL.equalsIgnoreCase(value)) {
      return quotedColumn + " IS NULL";
    }
    // escape single quotes to keep the produced clause well-formed
    return quotedColumn + " = '" + value.replace("'", "''") + "'";
  }

  private static List<String> split(String csv) {
    if (StringUtils.isBlank(csv)) {
      return new ArrayList<>();
    }
    return Arrays.stream(csv.split(",", -1)).map(String::trim).collect(Collectors.toList());
  }
}
