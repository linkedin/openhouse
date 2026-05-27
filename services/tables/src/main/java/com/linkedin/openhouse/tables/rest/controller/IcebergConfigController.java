package com.linkedin.openhouse.tables.rest.controller;

import com.linkedin.openhouse.tables.rest.adapter.IcebergRestJson;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Iceberg REST Catalog {@code GET /v1/config} endpoint.
 *
 * <p>Iceberg 1.5.2 {@link ConfigResponse.Builder} does not expose a {@code withEndpoints(...)}
 * method (endpoint advertisement was added in newer Iceberg). Clients will probe each call instead
 * and 404s for unsupported endpoints are tolerated by Spark/Flink REST catalog clients.
 */
@RestController
@Slf4j
public class IcebergConfigController {

  @GetMapping(value = "/iceberg/v1/config", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getConfig(
      @RequestParam(value = "warehouse", required = false) String warehouse) throws Exception {
    // Echo the caller's warehouse name back as `overrides.prefix` so the Iceberg
    // REST client routes every subsequent call through /v1/{prefix}/... — that is
    // the URL shape this adapter's namespace/table controllers serve.
    Map<String, String> overrides = new HashMap<>();
    overrides.put("prefix", warehouse == null || warehouse.isEmpty() ? "openhouse" : warehouse);
    ConfigResponse response =
        ConfigResponse.builder()
            .withDefaults(Collections.emptyMap())
            .withOverrides(overrides)
            .build();
    String json = IcebergRestJson.mapper().writeValueAsString(response);
    return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
  }
}
