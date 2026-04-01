package com.linkedin.openhouse.common.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Small request-scoped cache backed by {@link RequestAttributes}. When there is no active request,
 * cache lookups fall back to direct loading and writes are ignored.
 */
@Component
public class RequestScopedCache {

  private static final String ATTRIBUTE_NAME = RequestScopedCache.class.getName() + ".CACHE";

  public <T> T getOrLoad(String namespace, Object key, Supplier<T> loader) {
    RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
    if (requestAttributes == null) {
      return loader.get();
    }

    return cast(
        namespaceCache(requestAttributes, namespace).computeIfAbsent(key, unused -> loader.get()));
  }

  public void put(String namespace, Object key, Object value) {
    RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
    if (requestAttributes == null) {
      return;
    }

    namespaceCache(requestAttributes, namespace).put(key, value);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Map<Object, Object>> requestCache(RequestAttributes requestAttributes) {
    Map<String, Map<Object, Object>> cache =
        (Map<String, Map<Object, Object>>)
            requestAttributes.getAttribute(ATTRIBUTE_NAME, RequestAttributes.SCOPE_REQUEST);
    if (cache == null) {
      cache = new ConcurrentHashMap<>();
      requestAttributes.setAttribute(ATTRIBUTE_NAME, cache, RequestAttributes.SCOPE_REQUEST);
    }
    return cache;
  }

  private Map<Object, Object> namespaceCache(
      RequestAttributes requestAttributes, String namespace) {
    return requestCache(requestAttributes)
        .computeIfAbsent(namespace, unused -> new ConcurrentHashMap<>());
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object value) {
    return (T) value;
  }
}
