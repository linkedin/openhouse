package com.linkedin.openhouse.internal.catalog.model;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HouseTableTest {

  @Test
  public void testHouseTableDefaultValues() {
    HouseTable ht = HouseTable.builder().build();

    try {
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

          if (fieldName.equals("storageType")) {
            Assertions.assertEquals(StorageType.HDFS.getValue(), value);
          } else if (fieldName.equals("creationTime")) {
            Assertions.assertEquals(0L, value);
          } else if (fieldName.equals("lastModifiedTime")) {
            Assertions.assertEquals(0L, value);
          } else {
            Assertions.assertNull(value, getter.getName() + " is not null: " + value);
          }
        }
      }
    } catch (Exception e) {
      Assertions.fail(e);
    }
  }
}
