package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.services.ViewsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

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
}
