package com.linkedin.openhouse.tables.dto.mapper.attribute;

import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * This Converter class is required if {@link
 * com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository} is backed by in-memory
 * based backend like H2. Spring requires a {@link AttributeConverter} to save attribute {@link
 * TimePartitionSpec} in {@link com.linkedin.openhouse.tables.model.TableDto}
 */
@Converter
public class TimePartitionSpecConverter implements AttributeConverter<TimePartitionSpec, String> {
  @Override
  public String convertToDatabaseColumn(TimePartitionSpec attribute) {
    return (new Gson()).toJson(attribute);
  }

  @Override
  public TimePartitionSpec convertToEntityAttribute(String dbData) {
    return (new Gson()).fromJson(dbData, TimePartitionSpec.class);
  }
}
