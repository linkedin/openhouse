package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Startup lifecycle of the real {@link ViewsServiceImpl} in an isolated, closeable context: no
 * Spring Boot application, H2, shared context cache, or component scan. Only the service class is
 * registered; its collaborators are explicit mock singletons, so Spring performs the real field
 * injection and {@code @PostConstruct}. The cluster's default storage is resolved eagerly at
 * startup, as the removed view repository factory did, without selecting or allocating storage.
 */
public class ViewsServiceStartupTest {

  private final ViewsFeatureGate featureGate = mock(ViewsFeatureGate.class);
  private final HouseTableRepository houseTableRepository = mock(HouseTableRepository.class);
  private final ViewCommitEngine viewCommitEngine = mock(ViewCommitEngine.class);
  private final StorageSelector storageSelector = mock(StorageSelector.class);
  private final StorageManager storageManager = mock(StorageManager.class);
  private final FileIOManager fileIOManager = mock(FileIOManager.class);
  private final ViewMetadataCodec viewMetadataCodec = mock(ViewMetadataCodec.class);
  private final StorageType storageType = mock(StorageType.class);
  private final AuthorizationUtils authorizationUtils = mock(AuthorizationUtils.class);
  private final ViewWritePrivilegeMapper privilegeMapper = mock(ViewWritePrivilegeMapper.class);
  private final ClusterProperties clusterProperties = mock(ClusterProperties.class);

  @Test
  public void startupResolvesTheDefaultStorageOnceWithoutAnyViewOperation() {
    Storage defaultStorage = mock(Storage.class);
    when(storageManager.getDefaultStorage()).thenReturn(defaultStorage);

    try (AnnotationConfigApplicationContext context = isolatedContext()) {
      context.refresh();

      assertTrue(context.getBean(ViewsService.class) instanceof ViewsServiceImpl);
    }

    verify(storageManager, times(1)).getDefaultStorage();
    verifyNoInteractions(defaultStorage, storageSelector, viewCommitEngine, houseTableRepository);
  }

  /**
   * An unresolvable default storage is reachable only in an extended StorageType deployment (the
   * built-in types always resolve once StorageManager has validated its configuration), so it is
   * represented here by the isolated manager double. Startup fails with that exact cause.
   */
  @Test
  public void startupFailsWithTheDefaultStorageResolutionFailure() {
    IllegalArgumentException unresolvedDefault =
        new IllegalArgumentException("unresolved custom default storage");
    when(storageManager.getDefaultStorage()).thenThrow(unresolvedDefault);

    try (AnnotationConfigApplicationContext context = isolatedContext()) {
      BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);

      assertSame(unresolvedDefault, causeIdenticalTo(thrown, unresolvedDefault));
    }

    verify(storageManager, times(1)).getDefaultStorage();
    verifyNoInteractions(storageSelector, viewCommitEngine, houseTableRepository);
  }

  private AnnotationConfigApplicationContext isolatedContext() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    // Registered singletons bypass bean post-processing, so the mocks' own @Autowired fields stay
    // untouched while the service receives each of them by type.
    ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
    beanFactory.registerSingleton("viewsFeatureGate", featureGate);
    beanFactory.registerSingleton("houseTableRepository", houseTableRepository);
    beanFactory.registerSingleton("viewCommitEngine", viewCommitEngine);
    beanFactory.registerSingleton("storageSelector", storageSelector);
    beanFactory.registerSingleton("storageManager", storageManager);
    beanFactory.registerSingleton("fileIOManager", fileIOManager);
    beanFactory.registerSingleton("viewMetadataCodec", viewMetadataCodec);
    beanFactory.registerSingleton("storageType", storageType);
    beanFactory.registerSingleton("authorizationUtils", authorizationUtils);
    beanFactory.registerSingleton("viewWritePrivilegeMapper", privilegeMapper);
    beanFactory.registerSingleton("clusterProperties", clusterProperties);
    context.register(ViewsServiceImpl.class);
    return context;
  }

  private static Throwable causeIdenticalTo(Throwable thrown, Throwable expected) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause == expected) {
        return cause;
      }
    }
    return thrown;
  }
}
