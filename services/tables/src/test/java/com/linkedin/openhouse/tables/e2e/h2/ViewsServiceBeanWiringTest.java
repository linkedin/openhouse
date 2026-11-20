package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.services.ViewsService;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;

@SpringBootTest(classes = SpringH2Application.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ViewsServiceBeanWiringTest {

  @Autowired private ApplicationContext context;

  @Test
  public void viewApiRuntimeRegistersExactlyOneRealViewsService() {
    String[] names = context.getBeanNamesForType(ViewsService.class);

    assertEquals(1, names.length, "Exactly one ViewsService bean may be registered.");
    assertEquals(
        "ViewsServiceImpl",
        context.getType(names[0]).getSimpleName(),
        "The Iceberg-view-capable runtime must use the real bridge service, not the disabled stub.");
    assertFalse(
        context.containsBeanDefinition("viewsDisabledService"),
        "The disabled stub is only for runtimes without the Iceberg view API.");
  }

  /**
   * The service owns view capture, typed reads/delete and the engine call directly: no view
   * repository facade, its implementation, or its gated factory is registered. Matched by name, as
   * the removed types are no longer on the classpath.
   */
  @Test
  public void viewApiRuntimeRegistersNoViewRepositoryLayer() {
    Set<String> removedTypes =
        new HashSet<>(
            Arrays.asList(
                "com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository",
                "com.linkedin.openhouse.tables.repository.impl.OpenHouseInternalViewRepositoryImpl",
                "com.linkedin.openhouse.tables.repository.impl."
                    + "OpenHouseInternalViewRepositoryImpl$ViewsRepositoryConfig"));

    assertFalse(context.containsBean("openHouseInternalViewRepository"));
    for (String name : context.getBeanDefinitionNames()) {
      Class<?> type = context.getType(name);
      if (type == null) {
        continue;
      }
      assertFalse(removedTypes.contains(type.getName()), name + " is " + type.getName());
      for (Class<?> implemented : ClassUtils.getAllInterfacesForClassAsSet(type)) {
        assertFalse(
            removedTypes.contains(implemented.getName()),
            name + " implements " + implemented.getName());
      }
    }
  }
}
