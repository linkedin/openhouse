package com.linkedin.openhouse.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public class PageableUtilTest {

  @Test
  public void testGetSortByStr() {
    Pageable pageable = PageRequest.of(0, 2);
    String sortByStr = PageableUtil.getSortByStr(pageable);
    Assertions.assertEquals("", sortByStr);

    pageable = PageRequest.of(0, 2, Sort.by("id").descending());
    sortByStr = PageableUtil.getSortByStr(pageable);
    Assertions.assertEquals("id", sortByStr);
  }

  @Test
  public void testCreatePageable() {
    Pageable pageable = PageableUtil.createPageable(0, 2, null, null);
    Assertions.assertEquals("UNSORTED", pageable.getSort().toString());
    Assertions.assertEquals(0, pageable.getPageNumber());
    Assertions.assertEquals(2, pageable.getPageSize());

    pageable = PageableUtil.createPageable(0, 2, "id", null);
    Assertions.assertEquals("id: ASC", pageable.getSort().toString());

    pageable = PageableUtil.createPageable(0, 2, null, "data");
    Assertions.assertEquals("data: ASC", pageable.getSort().toString());
  }
}
