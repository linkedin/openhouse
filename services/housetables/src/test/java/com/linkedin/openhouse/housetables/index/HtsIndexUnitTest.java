package com.linkedin.openhouse.housetables.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

class HtsIndexUnitTest {
  static Stream<Method> explicitKeyQueries() {
    return explicitKeyQueries(UserTableHtsJdbcRepository.class);
  }

  private static Stream<Method> explicitKeyQueries(Class<?> repository) {
    return Arrays.stream(repository.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(Query.class))
        .filter(
            method ->
                method.getAnnotation(Query.class).value().contains(":databaseId")
                    || method.getAnnotation(Query.class).value().contains(":fromDatabaseId"));
  }

  static Stream<Method> derivedKeyQueries() {
    return Arrays.stream(UserTableHtsJdbcRepository.class.getDeclaredMethods())
        .filter(
            method ->
                java.lang.reflect.Modifier.isAbstract(method.getModifiers())
                    && !method.isSynthetic()
                    && !method.isAnnotationPresent(Query.class));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("derivedKeyQueries")
  void derivedKeyLookupsRetainCaseInsensitiveKeyPredicates(Method method) {
    PartTree query =
        new PartTree(method.getName(), com.linkedin.openhouse.housetables.model.UserTableRow.class);
    assertThat(query.getParts().toList()).isNotEmpty();
    for (Part part : query.getParts()) {
      if (Arrays.asList("databaseId", "tableId").contains(part.getProperty().toDotPath())) {
        assertThat(part.shouldIgnoreCase()).isNotEqualTo(Part.IgnoreCaseType.NEVER);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("explicitKeyQueries")
  void explicitQueriesMustMatchTheirTablesIndexExpressions(Method method) {
    Query query = method.getAnnotation(Query.class);
    for (String text : Arrays.asList(query.value(), query.countQuery())) {
      assertKeyExpression(method.getDeclaringClass(), text, method.toString());
    }
  }

  private static void assertKeyExpression(Class<?> repository, String text, String context) {
    boolean plainKey = repository == SoftDeletedUserTableHtsJdbcRepository.class;
    assertThat(text)
        .as(
            "%s: %s",
            context,
            plainKey
                ? "soft-delete PRIMARY KEY uses bare columns, not LOWER or UPPER"
                : "live-table functional key uses UPPER, not LOWER")
        .doesNotContainPattern(
            "(?i)"
                + (plainKey ? "(lower|upper)" : "lower")
                + "\\s*\\(\\s*\\w+\\.(databaseId|tableId)\\s*\\)");
  }

  @Test
  void softDeleteGuardDoesNotMistakeUpperForAPlainKeyFix() {
    String bare = "SELECT u FROM SoftDeletedUserTableRow u WHERE u.databaseId = :databaseId";
    assertKeyExpression(SoftDeletedUserTableHtsJdbcRepository.class, bare, "bare control");
    for (String function : Arrays.asList("lower", "upper")) {
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () ->
                  assertKeyExpression(
                      SoftDeletedUserTableHtsJdbcRepository.class,
                      bare.replace("u.databaseId =", function + "(u.databaseId) ="),
                      function))
          .isInstanceOf(AssertionError.class);
    }
    assertKeyExpression(
        UserTableHtsJdbcRepository.class,
        "SELECT u FROM UserTableRow u WHERE upper(u.databaseId) = upper(:databaseId)",
        "live-table control");
  }

  private static final Pattern UPPER_TABLE =
      Pattern.compile("(?i:upper)\\s*\\(\\s*u\\.entityType\\s*\\)\\s*=\\s*'TABLE'");

  private static final Pattern UPPER_VIEW =
      Pattern.compile("(?i:upper)\\s*\\(\\s*u\\.entityType\\s*\\)\\s*=\\s*'VIEW'");

  private static final Pattern NULL_ARM =
      Pattern.compile("u\\.entityType\\s+(?i:is\\s+null)\\s+(?i:or)\\s+" + UPPER_TABLE.pattern());

  private static final Pattern LOWER_ENTITY_TYPE =
      Pattern.compile("(?i)lower\\s*\\(\\s*u\\.entityType\\s*\\)");

  private static final Pattern WHERE = Pattern.compile("(?i)\\bwhere\\b");

  /** TABLE or VIEW for a typed repository method, null for neutral and general methods. */
  private static String typedEntity(Method method) {
    String name = method.getName();
    if (name.startsWith("findTableBy")
        || name.startsWith("findAllTablesBy")
        || name.startsWith("deleteTableBy")
        || name.equals("renameTableId")) {
      return "TABLE";
    }
    if (name.startsWith("findViewBy")
        || name.startsWith("findAllViewsBy")
        || name.startsWith("deleteViewBy")) {
      return "VIEW";
    }
    return null;
  }

  /**
   * The MySQL column collation is case-insensitive, so behavioural tests pass with or without
   * {@code upper(u.entityType)}. This reads the actual typed queries to keep the normalization, the
   * canonical literal and the table NULL arm in place regardless of backend collation.
   */
  @Test
  void typedQueriesRetainNormalizedEntityTypePredicates() {
    int tableQueries = 0;
    int viewQueries = 0;
    int countQueries = 0;
    for (Method method : UserTableHtsJdbcRepository.class.getDeclaredMethods()) {
      String entity = typedEntity(method);
      if (entity == null || method.isDefault() || method.isSynthetic()) {
        continue;
      }
      Query query = method.getAnnotation(Query.class);
      assertThat(query).as("%s must declare its typed @Query", method).isNotNull();
      assertTypedPredicate(query.value(), entity, method.toString());
      if (org.springframework.data.domain.Page.class.isAssignableFrom(method.getReturnType())) {
        assertThat(query.countQuery()).as("%s must declare its countQuery", method).isNotBlank();
      }
      if (!query.countQuery().isEmpty()) {
        assertTypedPredicate(query.countQuery(), entity, method + " countQuery");
        countQueries++;
      }
      if ("TABLE".equals(entity)) {
        tableQueries++;
      } else {
        viewQueries++;
      }
    }
    assertThat(tableQueries).as("table-typed query signatures").isEqualTo(7);
    assertThat(viewQueries).as("view-typed query signatures").isEqualTo(4);
    assertThat(countQueries).as("typed paged count queries").isEqualTo(4);
  }

  private static void assertTypedPredicate(String query, String entity, String context) {
    Matcher where = WHERE.matcher(query);
    assertThat(where.find()).as("%s: typed query has no WHERE clause", context).isTrue();
    String predicate = query.substring(where.end());
    boolean table = "TABLE".equals(entity);
    assertThat(predicate)
        .as("%s: compares upper(u.entityType) to the canonical '%s'", context, entity)
        .containsPattern(table ? UPPER_TABLE : UPPER_VIEW)
        .doesNotContainPattern(table ? UPPER_VIEW : UPPER_TABLE)
        .doesNotContainPattern(LOWER_ENTITY_TYPE);
    if (table) {
      assertThat(predicate).as("%s: keeps the legacy NULL arm", context).containsPattern(NULL_ARM);
    } else {
      assertThat(predicate)
          .as("%s: a view never matches a NULL discriminator", context)
          .doesNotContainPattern("(?i)u\\.entityType\\s+is\\s+null");
    }
  }
}
