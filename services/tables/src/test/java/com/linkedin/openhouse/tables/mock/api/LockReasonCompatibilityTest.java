package com.linkedin.openhouse.tables.mock.api;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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

  @Test
  void buildersDefaultToLegacy() {
    assertEquals(LockReason.LEGACY, CreateUpdateLockRequestBody.builder().build().getReason());
    assertEquals(LockReason.LEGACY, LockState.builder().build().getReason());
  }

  @Test
  void explicitNullBuilderReasonIsLegacy() {
    assertEquals(
        LockReason.LEGACY, CreateUpdateLockRequestBody.builder().reason(null).build().getReason());
    assertEquals(LockReason.LEGACY, LockState.builder().reason(null).build().getReason());
  }

  @Test
  void generatedClientDefaultsToLegacy() {
    assertEquals(
        "LEGACY",
        String.valueOf(
            new com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody()
                .getReason()));
    assertEquals(
        "LEGACY",
        String.valueOf(new com.linkedin.openhouse.tables.client.model.LockState().getReason()));
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
