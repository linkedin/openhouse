package com.linkedin.openhouse.housetables.mock.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.housetables.controller.UserHouseTablesController;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PutMapping;

/**
 * The generated client is produced from these annotations, so the declared response-code set of
 * every operation is part of the published contract rather than documentation. This pins the whole
 * surface at once: a new route, a dropped route, or a response code added to an existing operation
 * all fail here.
 *
 * <p>Two deliberate mismatches with runtime behavior are preserved rather than corrected, because
 * changing them would change the generated client: {@code PUT /hts/tables/restore} answers 200 but
 * advertises 204, and several operations can return a generic 500 that they do not advertise. The
 * scoped exception advice is {@code @Hidden} precisely so it adds none.
 */
public class UserHouseTablesControllerApiContractTest {

  private static final Map<String, Set<String>> EXPECTED_RESPONSE_CODES = frozenContract();

  private static Map<String, Set<String>> frozenContract() {
    Map<String, Set<String>> expected = new TreeMap<>();
    expected.put("GET /hts/tables", codes("200", "404"));
    expected.put("GET /hts/tables/query", codes("200"));
    expected.put("GET /v1/hts/tables/query", codes("200"));
    expected.put("GET /hts/tables/querySoftDeleted", codes("200", "400", "404"));
    expected.put("DELETE /hts/tables", codes("204", "400", "404"));
    expected.put("DELETE /v1/hts/tables", codes("204", "400", "404"));
    expected.put("PUT /hts/tables", codes("200", "201", "400", "404", "409"));
    expected.put("PATCH /hts/tables/rename", codes("204", "400", "404", "409"));
    expected.put("PUT /hts/tables/restore", codes("204", "400", "404", "409"));
    expected.put("DELETE /hts/tables/purge", codes("204", "400", "404"));
    // The six additions this change introduces.
    expected.put("GET /hts/entities", codes("200", "400", "404", "500"));
    expected.put("GET /hts/views", codes("200", "400", "404", "500"));
    expected.put("GET /hts/views/query", codes("200", "400", "500"));
    expected.put("GET /v1/hts/views/query", codes("200", "400", "500"));
    expected.put("PUT /hts/views", codes("200", "201", "400", "409"));
    expected.put("DELETE /hts/views", codes("204", "400", "404"));
    return expected;
  }

  private static Set<String> codes(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  @Test
  public void testEveryOperationDeclaresExactlyTheFrozenResponseCodes() {
    assertThat(declaredOperations()).containsExactlyInAnyOrderEntriesOf(EXPECTED_RESPONSE_CODES);
  }

  /** The six operations the generated client gains, named individually so a gap is legible. */
  @Test
  public void testTheSixNewOperationsArePresent() {
    assertThat(declaredOperations().keySet())
        .contains(
            "GET /hts/entities",
            "GET /hts/views",
            "GET /hts/views/query",
            "GET /v1/hts/views/query",
            "PUT /hts/views",
            "DELETE /hts/views");
  }

  /**
   * The view mutations must not gain a synthetic 500 just because the catch-all can produce one.
   * That is the specific regression a non-{@code @Hidden} scoped advice would cause.
   */
  @Test
  public void testViewMutationsDeclareNoServerErrorResponse() {
    Map<String, Set<String>> declared = declaredOperations();

    assertThat(declared.get("PUT /hts/views")).doesNotContain("500");
    assertThat(declared.get("DELETE /hts/views")).doesNotContain("500");
    assertThat(declared.get("PUT /hts/tables")).doesNotContain("500");
    assertThat(declared.get("DELETE /hts/tables")).doesNotContain("500");
    assertThat(declared.get("PATCH /hts/tables/rename")).doesNotContain("500");
  }

  private static Map<String, Set<String>> declaredOperations() {
    Map<String, Set<String>> declared = new HashMap<>();
    for (Method method : UserHouseTablesController.class.getDeclaredMethods()) {
      routeOf(method)
          .ifPresent(
              route ->
                  declared.put(
                      route,
                      Arrays.stream(method.getAnnotation(ApiResponses.class).value())
                          .map(ApiResponse::responseCode)
                          .collect(Collectors.toCollection(LinkedHashSet::new))));
    }
    return declared;
  }

  private static java.util.Optional<String> routeOf(Method method) {
    if (method.isAnnotationPresent(GetMapping.class)) {
      return java.util.Optional.of("GET " + method.getAnnotation(GetMapping.class).value()[0]);
    }
    if (method.isAnnotationPresent(PutMapping.class)) {
      return java.util.Optional.of("PUT " + method.getAnnotation(PutMapping.class).value()[0]);
    }
    if (method.isAnnotationPresent(DeleteMapping.class)) {
      return java.util.Optional.of(
          "DELETE " + method.getAnnotation(DeleteMapping.class).value()[0]);
    }
    if (method.isAnnotationPresent(PatchMapping.class)) {
      return java.util.Optional.of("PATCH " + method.getAnnotation(PatchMapping.class).value()[0]);
    }
    return java.util.Optional.empty();
  }
}
