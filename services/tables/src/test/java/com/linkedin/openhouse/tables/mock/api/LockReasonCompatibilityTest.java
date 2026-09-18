package com.linkedin.openhouse.tables.mock.api;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LockReasonCompatibilityTest {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @ParameterizedTest
  @ValueSource(strings = {"{\"locked\":true}", "{\"locked\":true,\"reason\":null}"})
  void missingOrNullApiReasonIsLegacy(String json) throws JsonProcessingException {
    assertEquals(
        LockReason.LEGACY,
        objectMapper.readValue(json, CreateUpdateLockRequestBody.class).getReason());
    assertEquals(LockReason.LEGACY, objectMapper.readValue(json, LockState.class).getReason());
  }

  @ParameterizedTest
  @ValueSource(strings = {"{\"locked\":true}", "{\"locked\":true,\"reason\":null}"})
  void existingStoredLocksReadAsLegacy(String json) {
    LockState lock =
        new PoliciesSpecMapper().toPoliciesObject("{\"lockState\":" + json + "}").getLockState();
    assertTrue(lock.isLocked());
    assertEquals(LockReason.LEGACY, lock.getReason());
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"locked\":false}", "{\"locked\":false,\"reason\":null}"})
  void inactiveStatesHaveNoDefaultReason(String json) throws JsonProcessingException {
    CreateUpdateLockRequestBody request =
        objectMapper.readValue(json, CreateUpdateLockRequestBody.class);
    LockState state = objectMapper.readValue(json, LockState.class);
    assertNull(request.getReason());
    assertNull(state.getReason());
    assertTrue(objectMapper.valueToTree(request).get("reason").isNull());
    assertTrue(objectMapper.valueToTree(state).get("reason").isNull());
    assertTrue(objectMapper.readTree(request.toJson()).get("reason").isNull());

    PoliciesSpecMapper mapper = new PoliciesSpecMapper();
    Policies policies = mapper.toPoliciesObject("{\"lockState\":" + json + "}");
    assertNull(policies.getLockState().getReason());
    String stored = mapper.toPoliciesJsonString(TableDto.builder().policies(policies).build());
    assertFalse(objectMapper.readTree(stored).get("lockState").hasNonNull("reason"));
    assertNull(mapper.toPoliciesObject(stored).getLockState().getReason());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void buildersDefaultOnlyWhenLocked(boolean locked) {
    LockReason expected = locked ? LockReason.LEGACY : null;
    assertEquals(
        expected, CreateUpdateLockRequestBody.builder().locked(locked).build().getReason());
    assertEquals(expected, LockState.builder().locked(locked).build().getReason());
    assertEquals(
        expected,
        CreateUpdateLockRequestBody.builder().locked(locked).reason(null).build().getReason());
    assertEquals(expected, LockState.builder().locked(locked).reason(null).build().getReason());
  }

  @ParameterizedTest
  @CsvSource({"true,LEGACY", "false,LEGACY", "true,SYSTEM_ONLY", "false,SYSTEM_ONLY"})
  void explicitReasonsSurviveSerialization(boolean locked, String reason)
      throws JsonProcessingException {
    String json = "{\"locked\":" + locked + ",\"reason\":\"" + reason + "\"}";
    CreateUpdateLockRequestBody request =
        assertDoesNotThrow(() -> objectMapper.readValue(json, CreateUpdateLockRequestBody.class));
    LockState state = assertDoesNotThrow(() -> objectMapper.readValue(json, LockState.class));
    assertEquals(reason, request.getReason().name());
    assertEquals(reason, state.getReason().name());
    assertEquals(reason, objectMapper.valueToTree(request).get("reason").asText());
    assertEquals(reason, objectMapper.valueToTree(state).get("reason").asText());
    assertEquals(reason, objectMapper.readTree(request.toJson()).get("reason").asText());
    assertEquals(
        request.getReason(),
        CreateUpdateLockRequestBody.builder()
            .locked(locked)
            .reason(request.getReason())
            .build()
            .getReason());
    assertEquals(
        state.getReason(),
        LockState.builder().locked(locked).reason(state.getReason()).build().getReason());

    PoliciesSpecMapper mapper = new PoliciesSpecMapper();
    Policies policies = mapper.toPoliciesObject("{\"lockState\":" + json + "}");
    assertEquals(state.getReason(), policies.getLockState().getReason());
    String stored = mapper.toPoliciesJsonString(TableDto.builder().policies(policies).build());
    assertEquals(state.getReason(), mapper.toPoliciesObject(stored).getLockState().getReason());
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"lockState\":null}"})
  void absentLockStateStaysAbsent(String json) {
    assertNull(new PoliciesSpecMapper().toPoliciesObject(json).getLockState());
  }

  @Test
  void reasonsDescribeGenericLockBehavior() {
    assertEquals(
        Arrays.asList("LEGACY", "SYSTEM_ONLY"),
        Arrays.stream(LockReason.values()).map(Enum::name).collect(Collectors.toList()));
  }

  @Test
  void generatedClientDoesNotSupplyReasonDefault() {
    com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody request =
        new com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody().locked(true);
    com.linkedin.openhouse.tables.client.model.LockState state =
        new com.linkedin.openhouse.tables.client.model.LockState().locked(false);
    assertNull(request.getReason());
    assertNull(state.getReason());
    assertFalse(request.getReason_JsonNullable().isPresent());
    assertFalse(state.getReason_JsonNullable().isPresent());
    ObjectMapper clientMapper = new ApiClient().getObjectMapper();
    assertFalse(clientMapper.valueToTree(request).has("reason"));
    assertFalse(clientMapper.valueToTree(state).has("reason"));
    request.reason(null);
    assertTrue(clientMapper.valueToTree(request).get("reason").isNull());
  }

  @Test
  void unknownApiReasonIsRejected() {
    assertThrows(
        JsonProcessingException.class,
        () ->
            objectMapper.readValue(
                "{\"locked\":true,\"reason\":\"FUTURE_REASON\"}",
                CreateUpdateLockRequestBody.class));
  }
}
