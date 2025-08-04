package com.linkedin.openhouse.common.utils;

import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public final class PageableUtil {
  private PageableUtil() {}

  public static String getSortByStr(Pageable pageable) {
    return pageable.getSort().stream()
        .map(Sort.Order::getProperty)
        .collect(Collectors.joining(","));
  }

  public static Pageable createPageable(int page, int size, String sortBy, String defaultSortBy) {
    Sort sort = Sort.unsorted();
    if (!StringUtils.isEmpty(sortBy)) {
      sort = Sort.by(sortBy).ascending();
    } else if (!StringUtils.isEmpty(defaultSortBy)) {
      sort = Sort.by(defaultSortBy).ascending();
    }
    return PageRequest.of(page, size, sort);
  }
}
