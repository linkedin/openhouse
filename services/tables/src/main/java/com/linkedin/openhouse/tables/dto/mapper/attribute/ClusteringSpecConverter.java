package com.linkedin.openhouse.tables.dto.mapper.attribute;

import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import java.util.Arrays;
import java.util.List;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * This Converter class is required if {@link
 * com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository} is backed by in-memory
 * based backend like H2. Spring requires a {@link AttributeConverter} to save attribute
 * ClusteringSpec in {@link com.linkedin.openhouse.tables.model.TableDto}
 */
@Converter
public class ClusteringSpecConverter implements AttributeConverter<List<ClusteringColumn>, String> {

  /**
   * Converts the value stored in the entity attribute into the data representation to be stored in
   * the database.
   *
   * @param attribute the entity attribute value to be converted
   * @return the converted data to be stored in the database column
   */
  @Override
  public String convertToDatabaseColumn(List<ClusteringColumn> attribute) {
    return (new Gson()).toJson(attribute);
  }

  /**
   * Converts the data stored in the database column into the value to be stored in the entity
   * attribute. Note that it is the responsibility of the converter writer to specify the correct
   * <code>dbData</code> type for the corresponding column for use by the JDBC driver: i.e.,
   * persistence providers are not expected to do such type conversion.
   *
   * @param dbData the data from the database column to be converted
   * @return the converted value to be stored in the entity attribute
   */
  @Override
  public List<ClusteringColumn> convertToEntityAttribute(String dbData) {
    return Arrays.asList((new Gson()).fromJson(dbData, ClusteringColumn[].class));
  }
}
