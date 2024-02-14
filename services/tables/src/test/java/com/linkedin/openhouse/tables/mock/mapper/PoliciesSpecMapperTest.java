package com.linkedin.openhouse.tables.mock.mapper;

import static com.linkedin.openhouse.tables.model.TableModelConstants.*;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PoliciesSpecMapperTest {

  @Autowired protected PoliciesSpecMapper policiesMapper;

  @Test
  public void testToPoliciesSpecJson() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .policies(TableModelConstants.TABLE_POLICIES)
                .build());
    String PoliciesSpec = policiesMapper.toPoliciesJsonString(tableDto);
    Assertions.assertEquals(
        (Integer) JsonPath.read(PoliciesSpec, "$.retention.count"),
        TableModelConstants.TABLE_POLICIES.getRetention().getCount());
  }

  @Test
  public void testToPoliciesSpecJsonWithNullPolicies() {
    TableDto tableDtoWithNullPolicies =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY.toBuilder().policies(null).build());
    String policiesSpec = policiesMapper.toPoliciesJsonString(tableDtoWithNullPolicies);
    Assertions.assertEquals(policiesSpec, "");
  }

  @Test
  public void testToPoliciesJsonFromObject() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .policies(TableModelConstants.TABLE_POLICIES)
                .build());
    String jsonPolicies = policiesMapper.toPoliciesJsonString(tableDto);
    Assertions.assertEquals(3, (Integer) JsonPath.read(jsonPolicies, "$.retention.count"));
  }

  @Test
  public void testEmptyPoliciesJsonFromObjectWithNullPolicy() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY.toBuilder().policies(null).build());

    String jsonPolicies = policiesMapper.toPoliciesJsonString(tableDto);
    Assertions.assertEquals("", jsonPolicies);
  }

  @Test
  public void testToPolicyObjectFromJson() {
    Policies Policies =
        policiesMapper.toPoliciesObject(new Gson().toJson(TableModelConstants.TABLE_POLICIES));
    Assertions.assertEquals(Policies, TableModelConstants.TABLE_POLICIES);
    Assertions.assertEquals(
        Policies.getRetention().getCount(),
        GET_TABLE_RESPONSE_BODY.getPolicies().getRetention().getCount());
  }

  private static String getBadJsonString() {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode node = factory.objectNode();
    ObjectNode retention = factory.objectNode();
    retention.put("days", 3);
    node.put("rention", retention);
    ObjectNode nodePolicies = factory.objectNode();
    nodePolicies.put("policies", node);
    return node.toString();
  }

  @Test
  public void testErrorPolicyObjectFromJson() {
    Policies Policies = policiesMapper.toPoliciesObject(getBadJsonString());
    Assertions.assertNotNull(Policies);
    Assertions.assertNull(Policies.getRetention());
  }
}
