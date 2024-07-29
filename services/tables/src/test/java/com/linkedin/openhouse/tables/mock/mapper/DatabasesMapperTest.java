package com.linkedin.openhouse.tables.mock.mapper;

import com.linkedin.openhouse.tables.dto.mapper.DatabasesMapper;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.DatabaseModelConstants;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class DatabasesMapperTest {
  @Autowired protected DatabasesMapper databasesMapper;

  @Autowired private ApplicationContext appContext;

  @Test
  public void tmp() {
    try {
      Object bean = appContext.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  @Test
  public void testToGetDatabaseResponseBody() {
    Assertions.assertEquals(
        DatabaseModelConstants.GET_DATABASE_RESPONSE_BODY,
        databasesMapper.toGetDatabaseResponseBody(DatabaseModelConstants.DATABASE_DTO));
  }

  @Test
  public void testToDatabaseDto() {
    DatabaseDto databaseDto = databasesMapper.toDatabaseDto("d1", "local-cluster");
    Assertions.assertEquals("d1", databaseDto.getDatabaseId());
    Assertions.assertEquals("local-cluster", databaseDto.getClusterId());
  }
}
