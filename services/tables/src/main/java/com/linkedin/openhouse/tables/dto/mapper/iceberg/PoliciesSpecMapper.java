package com.linkedin.openhouse.tables.dto.mapper.iceberg;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
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
}
