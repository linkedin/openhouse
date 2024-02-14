package com.linkedin.openhouse.common.schema;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.common.test.schema.ResourceIoHelper.*;
import static org.apache.iceberg.types.Types.NestedField.*;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergSchemaHelperTest {
  private Schema schema;
  private String schemaJson;

  @BeforeEach
  void setUp() throws Exception {
    this.schemaJson = getSchemaJsonFromResource("one-line-schema.json");
    this.schema =
        new Schema(
            required(1, "id", Types.StringType.get()), required(2, "name", Types.StringType.get()));
  }

  @Test
  public void testGetschemaFromSchemaJson() throws Exception {
    String schemaJsonSimple = getSchemaJsonFromResource("schema.json");
    String schemaJsonNoisy = getSchemaJsonFromResource("noisy-schema.json");

    Schema schemaSimple = getSchemaFromSchemaJson(schemaJsonSimple);
    Assertions.assertTrue(schema.sameSchema(schemaSimple));

    Schema schemaOneLine = getSchemaFromSchemaJson(schemaJson);
    Assertions.assertTrue(schema.sameSchema(schemaOneLine));

    Schema schemaNoisy = getSchemaFromSchemaJson(schemaJsonNoisy);
    Assertions.assertTrue(schema.sameSchema(schemaNoisy));
  }

  @Test
  public void testGetschemaJsonFromSchema() {
    String schemaJsonOneLine = getSchemaJsonFromSchema(schema);
    Assertions.assertTrue(schemaJson.equals(schemaJsonOneLine));
  }
}
