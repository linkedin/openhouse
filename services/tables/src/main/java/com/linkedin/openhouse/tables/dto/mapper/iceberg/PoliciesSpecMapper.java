package com.linkedin.openhouse.tables.dto.mapper.iceberg;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.common.DefaultColumnPattern;
import com.linkedin.openhouse.tables.model.TableDto;
import org.mapstruct.Mapper;
import org.mapstruct.Named;

@Mapper(componentModel = "spring")
public class PoliciesSpecMapper {

  /**
   * Given an Iceberg {@link TableDto}, serialize to JsonString format.
   *
   * @param tableDto an iceberg table
   * @return String if Policies object is set in TableDto otherwise ""
   */
  @Named("toPoliciesJsonString")
  public String toPoliciesJsonString(TableDto tableDto) throws JsonParseException {
    if (tableDto.getPolicies() != null) {
      try {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(tableDto.getPolicies());
      } catch (JsonParseException e) {
        throw new JsonParseException("Malformed policies json");
      }
    }
    return "";
  }

  /**
   * @param policiesString an openhouse table policies
   * @return Policies {@link Policies} Null if houseTable has no policies set
   */
  @Named("toPoliciesObject")
  public Policies toPoliciesObject(String policiesString) throws JsonParseException {
    if (policiesString.length() != 0) {
      try {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.fromJson(policiesString, Policies.class);
      } catch (JsonParseException e) {
        throw new JsonParseException(
            "Internal server error. Cannot convert policies Object to json");
      }
    }
    return null;
  }

  /**
   * mapPolicies is a mapStruct function which assigns default pattern in retention config if the
   * pattern is empty. Default values for pattern are defined at {@link DefaultColumnPattern} based
   * on granularity.
   *
   * @param policies for Openhouse table
   * @return mapped policies object
   */
  @Named("mapPolicies")
  public Policies mapPolicies(Policies policies) {
    String defaultPattern;
    if (policies != null
        && policies.getRetention() != null
        && policies.getRetention().getColumnPattern() != null
        && policies.getRetention().getColumnPattern().getPattern().isEmpty()) {
      if (policies
          .getRetention()
          .getGranularity()
          .name()
          .equals(DefaultColumnPattern.HOUR.toString())) {
        defaultPattern = DefaultColumnPattern.HOUR.getPattern();
      } else {
        defaultPattern = DefaultColumnPattern.DAY.getPattern();
      }
      Retention retentionPolicy =
          policies
              .getRetention()
              .toBuilder()
              .columnPattern(
                  policies
                      .getRetention()
                      .getColumnPattern()
                      .toBuilder()
                      .pattern(defaultPattern)
                      .build())
              .build();
      return policies.toBuilder().retention(retentionPolicy).build();
    } else {
      return policies;
    }
  }
}
