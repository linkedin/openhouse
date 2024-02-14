package com.linkedin.openhouse.tables.mock.mapper;

import com.linkedin.openhouse.tables.dto.mapper.DatabasesMapper;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.DatabaseModelConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class DatabasesMapperTest {
  @Autowired protected DatabasesMapper databasesMapper;

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
