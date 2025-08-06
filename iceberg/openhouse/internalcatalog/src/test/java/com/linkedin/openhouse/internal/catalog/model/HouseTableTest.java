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
}
