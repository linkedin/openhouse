package com.linkedin.openhouse.tables.dto.mapper.attribute;

import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

@Converter
public class PoliciesSpecConverter implements AttributeConverter<Policies, String> {
  public String convertToDatabaseColumn(Policies attribute) {
    return (new Gson()).toJson(attribute);
  }

  @Override
  public Policies convertToEntityAttribute(String dbData) {
    return (new Gson()).fromJson(dbData, Policies.class);
  }
}
