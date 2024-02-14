package com.linkedin.openhouse.jobs.model;

import com.google.gson.Gson;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

@Converter
public class JobConfConverter implements AttributeConverter<JobConf, String> {
  @Override
  public String convertToDatabaseColumn(JobConf attribute) {
    return (new Gson()).toJson(attribute);
  }

  @Override
  public JobConf convertToEntityAttribute(String dbData) {
    return (new Gson()).fromJson(dbData, JobConf.class);
  }
}
