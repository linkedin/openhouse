package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.authorization.Privileges;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Pins the authentication-only guard each /v1 views route is guarded by.
 *
 * <p>{@code @Secured} is enforced by a proxy at runtime, so a route that loses its annotation, or
 * has it silently retargeted at a resource privilege, still compiles and still serves traffic. This
 * test freezes the allow-all-read / service-authorized-write policy so that drift is a build
 * failure rather than an authorization regression.
 *
 * <p>Runs as a plain JUnit 5 reflection test: no Spring context is loaded, so the mapping stays
 * pinned independently of how method security happens to be wired.
 */
public class ViewsControllerPrivilegeTest {

  @Test
  public void everyViewRouteDeclaresOnlyTheAuthenticationGuard() throws NoSuchMethodException {
    Map<Method, String> expected = new LinkedHashMap<>();
    expected.put(
        ViewsController.class.getMethod("getView", String.class, String.class),
        Privileges.Privilege.AUTHENTICATED);
    expected.put(
        ViewsController.class.getMethod(
            "getAllViews",
            String.class,
            String.class,
            int.class,
            String.class,
            HttpServletRequest.class),
        Privileges.Privilege.AUTHENTICATED);
    expected.put(
        ViewsController.class.getMethod(
            "createView", String.class, CreateUpdateViewRequestBody.class),
        Privileges.Privilege.AUTHENTICATED);
    expected.put(
        ViewsController.class.getMethod(
            "updateView", String.class, String.class, CreateUpdateViewRequestBody.class),
        Privileges.Privilege.AUTHENTICATED);
    expected.put(
        ViewsController.class.getMethod("deleteView", String.class, String.class),
        Privileges.Privilege.AUTHENTICATED);

    for (Map.Entry<Method, String> route : expected.entrySet()) {
      Secured secured = route.getKey().getAnnotation(Secured.class);
      Assertions.assertNotNull(
          secured,
          "ViewsController." + route.getKey().getName() + " must stay guarded by @Secured.");
      Assertions.assertArrayEquals(
          new String[] {route.getValue()},
          secured.value(),
          "ViewsController."
              + route.getKey().getName()
              + " must require authentication only; reads perform no privilege check and writes are"
              + " authorized in ViewsServiceImpl.");
    }

    Assertions.assertEquals(
        expected.keySet().stream().map(Method::getName).collect(Collectors.toSet()),
        handlerMethodNames(),
        "Every request-mapped method on ViewsController must have its privilege pinned above.");
  }

  /**
   * Names of the methods Spring MVC would expose as routes. Derived from the mapping annotations
   * rather than a hard-coded list, so adding a route without pinning its privilege fails here.
   */
  private static Set<String> handlerMethodNames() {
    return Arrays.stream(ViewsController.class.getDeclaredMethods())
        .filter(
            method ->
                Arrays.stream(method.getAnnotations())
                    .anyMatch(
                        annotation ->
                            annotation.annotationType().isAnnotationPresent(RequestMapping.class)))
        .map(Method::getName)
        .collect(Collectors.toSet());
  }
}
