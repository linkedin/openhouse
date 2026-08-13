package com.linkedin.openhouse.internal.catalog.model;

import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HouseTableTest {

  @Test
  public void testHouseTableDefaultValues() {
    HouseTable ht = HouseTable.builder().build();

    try {
      // Get all field types to identify long fields
      Field[] fields = HouseTable.class.getDeclaredFields();
      Set<String> timestampFieldNames = new HashSet<>();
      for (Field field : fields) {
        if (field.getType() == long.class) {
          timestampFieldNames.add(field.getName());
        }
      }

      BeanInfo beanInfo = Introspector.getBeanInfo(HouseTable.class);
      PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

      for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
        if (propertyDescriptor.getReadMethod() != null) {
          Method getter = propertyDescriptor.getReadMethod();
          Object value = getter.invoke(ht);

          String fieldName =
              getter.getName().substring(3, 4).toLowerCase() + getter.getName().substring(4);
          if (!HouseTableSerdeUtils.HTS_FIELD_NAMES.contains(fieldName)) {
            // not a field getter
            continue;
          }

          // Check if this is a long field
          if (timestampFieldNames.contains(fieldName)) {
            // All long fields should have default value 0L
            Assertions.assertEquals(
                0L,
                value,
                String.format(
                    "Long field %s should have default value 0L but was %s", fieldName, value));
          } else {
            // Non-long fields should be null by default
            Assertions.assertNull(value, getter.getName() + " is not null: " + value);
          }
        }
      }
    } catch (Exception e) {
      Assertions.fail(e);
    }
  }

  /**
   * {@code HTS_FIELD_NAMES} is derived reflectively from HouseTable's private fields, and {@code
   * HouseTableMapper.extractRawHTSFields} only carries properties whose stripped key is in that
   * set. So the discriminator is only serialized through Iceberg table properties if it is a real
   * private field named exactly {@code entityType}, and its canonical property key must be {@code
   * openhouse.entityType}.
   */
  @Test
  public void testEntityTypeDefaultAndSerdeRegistration() {
    Assertions.assertNull(
        HouseTable.builder().build().getEntityType(),
        "entityType must default to null so ordinary table commits keep writing no discriminator");

    Assertions.assertTrue(
        HouseTableSerdeUtils.HTS_FIELD_NAMES.contains(HouseTableSerdeUtils.ENTITY_TYPE_FIELD_NAME),
        "entityType must be reflected into HTS_FIELD_NAMES: "
            + HouseTableSerdeUtils.HTS_FIELD_NAMES);

    Assertions.assertEquals("entityType", HouseTableSerdeUtils.ENTITY_TYPE_FIELD_NAME);
    Assertions.assertEquals(
        "openhouse.entityType",
        HouseTableSerdeUtils.getCanonicalFieldName(HouseTableSerdeUtils.ENTITY_TYPE_FIELD_NAME));

    Assertions.assertEquals("TABLE", HouseTableSerdeUtils.TABLE_ENTITY_TYPE);
    Assertions.assertEquals("VIEW", HouseTableSerdeUtils.VIEW_ENTITY_TYPE);
  }

  /**
   * Authoritative case-sensitivity contract. H2 (MODE=MySQL) is case-sensitive while production
   * MySQL default collation is not, so no SQL-level test can certify these semantics across
   * providers. These Java guards are what every point read, drop, rename, and occupancy check
   * actually consults, so they are pinned here independently of any database.
   *
   * <p>NULL and every spelling of TABLE classify as a table; every spelling of VIEW classifies as a
   * view; anything else is neither, so table APIs fail closed rather than treating an unknown
   * discriminator as a legacy table.
   *
   * <p>The empty-string row goes beyond the plan, which only named NULL/TABLE/VIEW/garbage. It is
   * included deliberately because {@code entity_type} is a nullable {@code VARCHAR} that can hold
   * {@code ''}, and "unknown non-null fails closed" must cover it. The natural implementation
   * ({@code entityType == null || entityType.equalsIgnoreCase(TABLE)}) satisfies it for free —
   * implementers must not special-case {@code ""} as blank/absent.
   */
  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {
        "NULL,  true,  false",
        "TABLE, true,  false",
        "table, true,  false",
        "TaBlE, true,  false",
        "VIEW,  false, true",
        "view,  false, true",
        "ViEw,  false, true",
        "UNKNOWN, false, false",
        "'', false, false"
      })
  public void testEntityTypeClassification(
      String entityType, boolean expectedTable, boolean expectedView) {
    Assertions.assertEquals(
        expectedTable,
        HouseTableSerdeUtils.isTableEntityType(entityType),
        "isTableEntityType(" + entityType + ")");
    Assertions.assertEquals(
        expectedView,
        HouseTableSerdeUtils.isViewEntityType(entityType),
        "isViewEntityType(" + entityType + ")");

    // The same classification must hold when read off a real pointer row.
    HouseTable row =
        HouseTable.builder().databaseId("d1").tableId("t1").entityType(entityType).build();
    Assertions.assertEquals(
        expectedTable, HouseTableSerdeUtils.isTableEntityType(row.getEntityType()));
    Assertions.assertEquals(
        expectedView, HouseTableSerdeUtils.isViewEntityType(row.getEntityType()));
  }
}
