package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.spec.model.StorageLocation;
import com.linkedin.openhouse.housetables.service.StorageLocationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** REST controller for {@link StorageLocation} management in HouseTables Service. */
@RestController
public class StorageLocationController {

  private static final String STORAGE_LOCATIONS_ENDPOINT = "/hts/storageLocations";

  @Autowired private StorageLocationService storageLocationService;

  @Operation(
      summary = "Create a StorageLocation",
      description =
          "Creates a new StorageLocation with the given URI and returns the persisted entity.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "StorageLocation POST: CREATED"),
        @ApiResponse(responseCode = "400", description = "StorageLocation POST: BAD_REQUEST")
      })
  @PostMapping(
      value = STORAGE_LOCATIONS_ENDPOINT,
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<StorageLocation> createStorageLocation(
      @Valid @RequestBody StorageLocation request) {
    StorageLocation created = storageLocationService.createStorageLocation(request.getUri());
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  @Operation(
      summary = "Create a StorageLocation without URI",
      description = "Creates a new StorageLocation with an empty URI. Use PATCH to set the URI.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {@ApiResponse(responseCode = "201", description = "StorageLocation POST: CREATED")})
  @PostMapping(
      value = STORAGE_LOCATIONS_ENDPOINT + "/allocate",
      produces = {"application/json"})
  public ResponseEntity<StorageLocation> allocateStorageLocation() {
    StorageLocation created = storageLocationService.createStorageLocation();
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  @Operation(
      summary = "Update a StorageLocation URI",
      description = "Updates the URI of an existing StorageLocation.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "StorageLocation PATCH: OK"),
        @ApiResponse(responseCode = "404", description = "StorageLocation PATCH: NOT_FOUND")
      })
  @PatchMapping(
      value = STORAGE_LOCATIONS_ENDPOINT + "/{storageLocationId}",
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<StorageLocation> updateStorageLocationUri(
      @PathVariable String storageLocationId, @Valid @RequestBody StorageLocation request) {
    StorageLocation updated =
        storageLocationService.updateStorageLocationUri(storageLocationId, request.getUri());
    return ResponseEntity.ok(updated);
  }

  @Operation(
      summary = "Get a StorageLocation by ID",
      description = "Returns the StorageLocation identified by storageLocationId.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "StorageLocation GET: OK"),
        @ApiResponse(responseCode = "404", description = "StorageLocation GET: NOT_FOUND")
      })
  @GetMapping(
      value = STORAGE_LOCATIONS_ENDPOINT + "/{storageLocationId}",
      produces = {"application/json"})
  public ResponseEntity<StorageLocation> getStorageLocation(
      @PathVariable String storageLocationId) {
    return ResponseEntity.ok(storageLocationService.getStorageLocation(storageLocationId));
  }

  @Operation(
      summary = "Get StorageLocations for a table",
      description = "Returns all StorageLocations associated with the given table.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {@ApiResponse(responseCode = "200", description = "StorageLocation GET: OK")})
  @GetMapping(
      value = STORAGE_LOCATIONS_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<List<StorageLocation>> getStorageLocationsForTable(
      @RequestParam String databaseId, @RequestParam String tableId) {
    return ResponseEntity.ok(
        storageLocationService.getStorageLocationsForTable(databaseId, tableId));
  }

  @Operation(
      summary = "Associate a StorageLocation with a table",
      description = "Links an existing StorageLocation to the given table.",
      tags = {"StorageLocation"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "StorageLocation LINK: NO_CONTENT"),
        @ApiResponse(responseCode = "404", description = "StorageLocation LINK: NOT_FOUND")
      })
  @PostMapping(value = STORAGE_LOCATIONS_ENDPOINT + "/link")
  public ResponseEntity<Void> linkStorageLocationToTable(
      @RequestParam String databaseId,
      @RequestParam String tableId,
      @RequestParam String storageLocationId) {
    storageLocationService.addStorageLocationToTable(databaseId, tableId, storageLocationId);
    return ResponseEntity.noContent().build();
  }
}
