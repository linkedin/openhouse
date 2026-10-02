package com.linkedin.openhouse.housetables.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.JobsServiceImpl;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

class HtsIndexUnitTest {
  @Test
  void everyHtsRouteHasAnExplicitIndexPolicy() throws Exception {
    Set<String> routes = new HashSet<>();
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);
    scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));
    for (org.springframework.beans.factory.config.BeanDefinition bean :
        scanner.findCandidateComponents("com.linkedin.openhouse.housetables.controller")) {
      Class<?> controller = Class.forName(bean.getBeanClassName());
      assertThat(AnnotatedElementUtils.findMergedAnnotation(controller, RequestMapping.class))
          .as("Update route discovery for class-level mappings")
          .isNull();
      for (Method method : controller.getDeclaredMethods()) {
        RequestMapping mapping =
            AnnotatedElementUtils.findMergedAnnotation(method, RequestMapping.class);
        if (mapping != null) {
          for (String path : mapping.path()) {
            for (org.springframework.web.bind.annotation.RequestMethod verb : mapping.method()) {
              routes.add(verb.name() + " " + path);
            }
          }
        }
      }
    }
    assertThat(routes).isNotEmpty();
    assertThat(
            HtsIndexScenarios.all().stream()
                .map(HtsIndexScenarios::route)
                .collect(Collectors.toSet()))
        .containsExactlyInAnyOrderElementsOf(routes);
    assertThat(HtsIndexScenarios.all().stream().map(s -> s.name).distinct().count())
        .isEqualTo(HtsIndexScenarios.all().size());
    for (HtsIndexScenarios scenario : HtsIndexScenarios.all()) {
      assertThat(scenario.tables).isNotEmpty();
      if (scenario.rowBudget == 0) {
        assertThat(scenario.scanReason).isNotBlank();
      } else {
        assertThat(scenario.rowBudget).isBetween(1, 300);
        assertThat(scenario.scanReason).isNull();
      }
    }
  }

  static Stream<Method> explicitKeyQueries() {
    return explicitKeyQueries(UserTableHtsJdbcRepository.class);
  }

  static Stream<Method> softDeleteKeyQueries() {
    return explicitKeyQueries(SoftDeletedUserTableHtsJdbcRepository.class);
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

  // TODO: Fix soft-delete access patterns to use the plain composite primary key, then re-enable.
  @Disabled("Pending soft-delete primary-key access-pattern fix")
  @ParameterizedTest(name = "{0}")
  @MethodSource("softDeleteKeyQueries")
  void softDeleteQueriesMustMatchTheirPrimaryKey(Method method) {
    explicitQueriesMustMatchTheirTablesIndexExpressions(method);
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

  // TODO: Fix job-ID query access patterns to avoid enumerating job_row, then re-enable.
  @Disabled("Pending indexed job-ID query access-pattern fix")
  @Test
  @SuppressWarnings("unchecked")
  void jobIdFilteredEndpointMustNotEnumerateAllJobs() {
    HtsRepository<JobRow, JobRowPrimaryKey> repository = mock(HtsRepository.class);
    JobsServiceImpl service = new JobsServiceImpl();
    ReflectionTestUtils.setField(service, "htsRepository", repository);
    ReflectionTestUtils.setField(
        service, "jobMapper", org.mapstruct.factory.Mappers.getMapper(JobMapper.class));
    JobRow row = JobRow.builder().jobId("job04200").version(1L).build();
    when(repository.findAll()).thenReturn(Collections.singletonList(row));
    when(repository.findById(JobRowPrimaryKey.builder().jobId("job04200").build()))
        .thenReturn(java.util.Optional.of(row));
    assertThat(service.getAllJobs(Job.builder().jobId("job04200").build()))
        .extracting(com.linkedin.openhouse.housetables.dto.model.JobDto::getJobId)
        .containsExactly("job04200");
    verify(repository, never()).findAll();
  }

  @ParameterizedTest
  @ValueSource(strings = {"ALL", "index", "index_merge", ""})
  void rejectsTableAndFullIndexScansEvenWhenAnIndexNameIsPresent(String access) throws Exception {
    assertThat(MysqlIndexPlan.violations(plan(access, "PRIMARY", 1), 10)).isNotEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"const", "eq_ref", "ref", "range"})
  void acceptsSelectiveAccessWithBoundedWork(String access) throws Exception {
    assertThat(MysqlIndexPlan.violations(plan(access, "upper_key", 1), 10)).isEmpty();
  }

  @Test
  void rejectsUnboundedRangesMissingEvidenceAndNestedScans() throws Exception {
    assertThat(MysqlIndexPlan.violations(plan("range", "upper_key", 10000), 300)).isNotEmpty();
    assertThat(
            MysqlIndexPlan.violations(
                new ObjectMapper()
                    .readTree("{\"query_block\":{\"message\":\"Select tables optimized away\"}}"),
                10))
        .isNotEmpty();
    assertThat(
            MysqlIndexPlan.violations(
                new ObjectMapper()
                    .readTree(
                        "{\"query_block\":{\"ordering_operation\":{\"table\":"
                            + "{\"table_name\":\"t\",\"access_type\":\"ref\",\"rows_examined_per_scan\":1}}}}"),
                10))
        .isNotEmpty();
    assertThat(
            MysqlIndexPlan.violations(
                new ObjectMapper()
                    .readTree(
                        "{\"query_block\":{\"nested_loop\":[{\"table\":"
                            + "{\"table_name\":\"a\",\"access_type\":\"const\",\"key\":\"PRIMARY\","
                            + "\"rows_examined_per_scan\":1}},{\"table\":{\"table_name\":\"b\","
                            + "\"access_type\":\"ALL\",\"rows_examined_per_scan\":10000}}]}}"),
                10))
        .isNotEmpty();
  }

  @Test
  void acceptsOnlyExplicitConstantIndexMissesNotGenericImpossiblePredicates() throws Exception {
    ObjectMapper json = new ObjectMapper();
    assertThat(
            MysqlIndexPlan.violations(
                json.readTree("{\"query_block\":{\"message\":\"no matching row in const table\"}}"),
                10))
        .isEmpty();
    assertThat(
            MysqlIndexPlan.violations(
                json.readTree("{\"query_block\":{\"message\":\"Impossible WHERE\"}}"), 10))
        .isNotEmpty();
  }

  private static com.fasterxml.jackson.databind.JsonNode plan(String access, String key, int rows)
      throws Exception {
    return new ObjectMapper()
        .readTree(
            "{\"query_block\":{\"table\":{\"table_name\":\"t\","
                + "\"access_type\":\""
                + access
                + "\",\"key\":\""
                + key
                + "\",\"rows_examined_per_scan\":"
                + rows
                + "}}}");
  }
}
