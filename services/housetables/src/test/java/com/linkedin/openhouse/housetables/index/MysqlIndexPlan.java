package com.linkedin.openhouse.housetables.index;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** Test contract for selective access, not merely the presence of an index in a plan. */
final class MysqlIndexPlan {
  private MysqlIndexPlan() {}

  static List<String> violations(JsonNode plan, int maxRows) {
    List<String> violations = new ArrayList<>();
    List<JsonNode> tables = new ArrayList<>();
    collectTables(plan, tables);
    // An absent key can mean a folded COUNT/constant or a missing row, not a healthy lookup.
    if (tables.isEmpty() && !constantMiss(plan)) {
      violations.add("No verifiable table access in plan");
    }

    for (JsonNode table : tables) {
      String access = table.path("access_type").asText();
      String name = table.path("table_name").asText();
      if (!Arrays.asList("const", "eq_ref", "ref", "range").contains(access)) {
        violations.add(name + ": non-selective access_type=" + access);
      }
      if (!table.hasNonNull("key")) {
        violations.add(name + ": no chosen index");
      }
      if (!table.has("rows_examined_per_scan")) {
        violations.add(name + ": missing rows_examined_per_scan");
      } else if (table.path("rows_examined_per_scan").asLong() > maxRows) {
        violations.add(
            name
                + ": examines "
                + table.get("rows_examined_per_scan")
                + " rows; budget="
                + maxRows);
      }
    }
    return violations;
  }

  private static boolean constantMiss(JsonNode node) {
    String message = node.path("message").asText();
    if (message.equalsIgnoreCase("no matching row in const table")
        || message.equalsIgnoreCase("Impossible WHERE noticed after reading const tables")) {
      return true;
    }
    for (JsonNode child : node) {
      if (constantMiss(child)) {
        return true;
      }
    }
    return false;
  }

  static List<JsonNode> tables(JsonNode plan) {
    List<JsonNode> result = new ArrayList<>();
    collectTables(plan, result);
    return result;
  }

  private static void collectTables(JsonNode node, List<JsonNode> result) {
    if (node.isObject() && node.has("table_name")) {
      result.add(node);
    }
    Iterator<JsonNode> children = node.elements();
    while (children.hasNext()) {
      collectTables(children.next(), result);
    }
  }
}
