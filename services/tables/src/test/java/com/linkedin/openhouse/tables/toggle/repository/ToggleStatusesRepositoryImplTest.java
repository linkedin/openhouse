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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
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

    RequestContextHolder.setRequestAttributes(
        new ServletRequestAttributes(new MockHttpServletRequest()));
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
}
