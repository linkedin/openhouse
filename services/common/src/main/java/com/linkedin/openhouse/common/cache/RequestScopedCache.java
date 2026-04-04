package com.linkedin.openhouse.common.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Small request-scoped cache backed by Spring's cache abstraction. A {@link CaffeineCacheManager}
 * instance is bound to the active {@link RequestAttributes}, so each request gets isolated caches
 * while the public API stays simple. When there is no active request, cache lookups fall back to
 * direct loading and writes are ignored.
 *
 * <p>This class intentionally lives as a plain shared utility instead of a scanned Spring
 * component. OpenHouse applications use explicit package scan allowlists, and this package is not
 * included consistently across all application and test contexts. The default bean is therefore
 * provided from a scanned configuration class, which keeps the utility in {@code services/common}
 * while still allowing downstream repos to override the bean if needed. The cache manager is
 * created lazily per request so future wiring can swap in a differently configured Spring {@link
 * CacheManager} without changing call sites.
 */
public class RequestScopedCache {

  private static final String ATTRIBUTE_NAME =
      RequestScopedCache.class.getName() + ".CACHE_MANAGER";
  private final Supplier<CacheManager> cacheManagerFactory;

  public RequestScopedCache() {
    this(RequestScopedCache::defaultCacheManager);
  }

  public RequestScopedCache(Supplier<CacheManager> cacheManagerFactory) {
    this.cacheManagerFactory = Objects.requireNonNull(cacheManagerFactory);
  }

  public <T> T getOrLoad(String namespace, Object key, Supplier<T> loader) {
    Cache cache = currentCache(namespace);
    if (cache == null) {
      return loader.get();
    }
    return cache.get(key, loader::get);
  }

  public void put(String namespace, Object key, Object value) {
    Cache cache = currentCache(namespace);
    if (cache == null) {
      return;
    }
    cache.put(key, value);
  }

  private Cache currentCache(String namespace) {
    RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
    if (requestAttributes == null) {
      return null;
    }
    CacheManager cacheManager = requestCacheManager(requestAttributes);
    if (cacheManager == null) {
      return null;
    }
    return cacheManager.getCache(namespace);
  }

  private CacheManager requestCacheManager(RequestAttributes requestAttributes) {
    CacheManager cacheManager =
        (CacheManager)
            requestAttributes.getAttribute(ATTRIBUTE_NAME, RequestAttributes.SCOPE_REQUEST);
    if (cacheManager == null) {
      cacheManager = cacheManagerFactory.get();
      requestAttributes.setAttribute(ATTRIBUTE_NAME, cacheManager, RequestAttributes.SCOPE_REQUEST);
    }
    return cacheManager;
  }

  private static CacheManager defaultCacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCaffeine(Caffeine.newBuilder().recordStats());
    return cacheManager;
  }
}
