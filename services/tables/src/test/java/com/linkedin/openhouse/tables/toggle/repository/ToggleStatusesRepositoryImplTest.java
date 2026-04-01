package com.linkedin.openhouse.tables.toggle.repository;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.cache.RequestScopedCache;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyToggleStatus;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.toggle.ToggleStatusMapper;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ToggleStatusesRepositoryImplTest {

  @Mock private ToggleStatusApi apiInstance;
  @Mock private ToggleStatusMapper toggleStatusMapper;
  @Mock private EntityResponseBodyToggleStatus entityResponseBodyToggleStatus;
  @Mock private ToggleStatus toggleStatus;

  @Spy private RequestScopedCache requestScopedCache = new RequestScopedCache();

  @InjectMocks private ToggleStatusesRepositoryImpl toggleStatusesRepository;

  @Test
  void testFindByIdUsesRequestCacheWithinSingleRequest() {
    ToggleStatusKey toggleStatusKey =
        ToggleStatusKey.builder().databaseId("db").tableId("table").featureId("feature").build();
    TableToggleStatus expected =
        TableToggleStatus.builder()
            .databaseId("db")
            .tableId("table")
            .featureId("feature")
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();

    when(apiInstance.getTableToggleStatus("db", "table", "feature"))
        .thenReturn(Mono.just(entityResponseBodyToggleStatus));
    when(entityResponseBodyToggleStatus.getEntity()).thenReturn(toggleStatus);
    when(toggleStatusMapper.toTableToggleStatus(toggleStatusKey, toggleStatus))
        .thenReturn(expected);

    RequestContextHolder.setRequestAttributes(new MapBackedRequestAttributes());
    try {
      TableToggleStatus first = toggleStatusesRepository.findById(toggleStatusKey).orElseThrow();
      TableToggleStatus second = toggleStatusesRepository.findById(toggleStatusKey).orElseThrow();

      assertSame(expected, first);
      assertSame(expected, second);
    } finally {
      RequestContextHolder.resetRequestAttributes();
    }

    verify(apiInstance, times(1)).getTableToggleStatus("db", "table", "feature");
  }

  private static final class MapBackedRequestAttributes implements RequestAttributes {
    private final HashMap<String, Object> requestScope = new HashMap<>();

    @Override
    public Object getAttribute(String name, int scope) {
      return scope == RequestAttributes.SCOPE_REQUEST ? requestScope.get(name) : null;
    }

    @Override
    public void setAttribute(String name, Object value, int scope) {
      if (scope == RequestAttributes.SCOPE_REQUEST) {
        requestScope.put(name, value);
      }
    }

    @Override
    public void removeAttribute(String name, int scope) {
      if (scope == RequestAttributes.SCOPE_REQUEST) {
        requestScope.remove(name);
      }
    }

    @Override
    public String[] getAttributeNames(int scope) {
      return scope == RequestAttributes.SCOPE_REQUEST
          ? requestScope.keySet().toArray(new String[0])
          : new String[0];
    }

    @Override
    public void registerDestructionCallback(String name, Runnable callback, int scope) {}

    @Override
    public Object resolveReference(String key) {
      return null;
    }

    @Override
    public String getSessionId() {
      return "test-session";
    }

    @Override
    public Object getSessionMutex() {
      return this;
    }
  }
}
