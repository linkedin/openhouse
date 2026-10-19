package com.linkedin.openhouse.housetables.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * Explains Hibernate's bound statement on the same connection BEFORE execution. In particular,
 * explaining a DELETE after execution could mistake "no matching row" for indexed access.
 */
final class ExplainingDataSource extends DelegatingDataSource {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final ThreadLocal<List<Plan>> CAPTURE = new ThreadLocal<>();

  static final class Plan {
    public final String sql;
    public final List<String> bindings;
    public final JsonNode explain;

    Plan(String sql, List<String> bindings, JsonNode explain) {
      this.sql = sql;
      this.bindings = bindings;
      this.explain = explain;
    }
  }

  ExplainingDataSource(DataSource delegate) {
    super(delegate);
  }

  static void begin() {
    if (CAPTURE.get() != null) {
      throw new IllegalStateException("Nested SQL capture");
    }
    CAPTURE.set(new ArrayList<>());
  }

  static List<Plan> end() {
    List<Plan> plans = CAPTURE.get();
    CAPTURE.remove();
    if (plans == null) {
      throw new IllegalStateException("SQL capture was not started");
    }
    return plans;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return wrap(super.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return wrap(super.getConnection(username, password));
  }

  private Connection wrap(Connection connection) {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (CAPTURE.get() != null
                  && (method.getName().equals("createStatement")
                      || method.getName().equals("prepareCall")
                      || method.getName().equals("unwrap"))) {
                throw new SQLException(
                    "Uncaptured JDBC access during endpoint: " + method.getName());
              }
              Object result = invoke(connection, method, args);
              if (method.getName().equals("prepareStatement")) {
                return wrapStatement(connection, (PreparedStatement) result, (String) args[0]);
              }
              return result;
            });
  }

  private PreparedStatement wrapStatement(
      Connection connection, PreparedStatement statement, String sql) {
    Map<Integer, Binding> bindings = new LinkedHashMap<>();
    return (PreparedStatement)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (name.startsWith("set")
                  && args != null
                  && args.length >= 2
                  && args[0] instanceof Integer) {
                bindings.put((Integer) args[0], new Binding(method, args.clone()));
              } else if (name.equals("clearParameters")) {
                bindings.clear();
              } else if (name.equals("addBatch") && CAPTURE.get() != null) {
                throw new SQLException("Batched endpoint SQL is not supported by plan capture");
              } else if (name.startsWith("execute") && CAPTURE.get() != null) {
                capture(connection, sql, bindings);
              }
              return invoke(statement, method, args);
            });
  }

  private static void capture(Connection connection, String sql, Map<Integer, Binding> bindings)
      throws Throwable {
    String normalized = sql.trim().toLowerCase(Locale.ROOT);
    if (normalized.startsWith("insert ")) {
      // An INSERT has no row-selection plan; read-before-write and UPDATE/DELETE are checked.
      return;
    }
    if (!(normalized.startsWith("select ")
        || normalized.startsWith("update ")
        || normalized.startsWith("delete "))) {
      throw new SQLException("Unclassified endpoint SQL: " + sql);
    }
    List<String> parameters = new ArrayList<>();
    for (Map.Entry<Integer, Binding> entry : bindings.entrySet()) {
      parameters.add(
          entry.getKey()
              + ":"
              + entry.getValue().method.getName()
              + "="
              + java.util.Arrays.toString(entry.getValue().arguments));
    }
    JsonNode plan = explain(connection, sql, bindings);
    // MySQL can fold COUNT on a unique key into a constant. Preserve that plan, and explain
    // the identical bound predicate with a plain projection to prove how the row was located.
    if (normalized.matches("(?s)^select\\s+count\\(.*")
        && MysqlIndexPlan.tables(plan).isEmpty()
        && plan.toString().contains("Select tables optimized away")) {
      String probe = sql.replaceFirst("(?is)^select\\s+count\\([^)]*\\)", "select 1");
      com.fasterxml.jackson.databind.node.ObjectNode evidence = JSON.createObjectNode();
      evidence.set("original", plan);
      evidence.set("accessProbe", explain(connection, probe, bindings));
      plan = evidence;
    }
    CAPTURE.get().add(new Plan(sql, parameters, plan));
  }

  private static JsonNode explain(Connection connection, String sql, Map<Integer, Binding> bindings)
      throws Throwable {
    try (PreparedStatement explain = connection.prepareStatement("EXPLAIN FORMAT=JSON " + sql)) {
      for (Map.Entry<Integer, Binding> entry : bindings.entrySet()) {
        Binding binding = entry.getValue();
        invoke(explain, binding.method, binding.arguments);
      }
      try (ResultSet result = explain.executeQuery()) {
        if (!result.next()) {
          throw new SQLException("EXPLAIN returned no plan: " + sql);
        }
        return JSON.readTree(result.getString(1));
      }
    }
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException error) {
      throw error.getCause();
    }
  }

  private static final class Binding {
    final Method method;
    final Object[] arguments;

    Binding(Method method, Object[] arguments) {
      this.method = method;
      this.arguments = arguments;
    }
  }
}
