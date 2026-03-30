package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.StorageLocationDto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Calls HouseTables Service StorageLocation endpoints via HTTP. Uses the HTS WebClient configured
 * in the application.
 */
@Slf4j
public class StorageLocationRepositoryImpl implements StorageLocationRepository {

  private static final String STORAGE_LOCATIONS_PATH = "/hts/storageLocations";

  private final WebClient htsWebClient;

  public StorageLocationRepositoryImpl(WebClient htsWebClient) {
    this.htsWebClient = htsWebClient;
  }

  @Override
  public StorageLocationDto createStorageLocation(String uri) {
    Map<String, String> body = Map.of("uri", uri);
    Map<?, ?> response =
        htsWebClient
            .post()
            .uri(STORAGE_LOCATIONS_PATH)
            .bodyValue(body)
            .retrieve()
            .bodyToMono(Map.class)
            .block();
    return toDto(response);
  }

  @Override
  public StorageLocationDto allocateStorageLocation() {
    Map<?, ?> response =
        htsWebClient
            .post()
            .uri(STORAGE_LOCATIONS_PATH + "/allocate")
            .retrieve()
            .bodyToMono(Map.class)
            .block();
    return toDto(response);
  }

  @Override
  public StorageLocationDto updateStorageLocationUri(String storageLocationId, String uri) {
    Map<String, String> body = Map.of("uri", uri);
    Map<?, ?> response =
        htsWebClient
            .patch()
            .uri(STORAGE_LOCATIONS_PATH + "/{id}", storageLocationId)
            .bodyValue(body)
            .retrieve()
            .bodyToMono(Map.class)
            .block();
    return toDto(response);
  }

  @Override
  public StorageLocationDto getStorageLocation(String storageLocationId) {
    Map<?, ?> response =
        htsWebClient
            .get()
            .uri(STORAGE_LOCATIONS_PATH + "/{id}", storageLocationId)
            .retrieve()
            .bodyToMono(Map.class)
            .block();
    return toDto(response);
  }

  @Override
  public List<StorageLocationDto> getStorageLocationsForTable(String tableUuid) {
    List<Map<?, ?>> response =
        htsWebClient
            .get()
            .uri(
                uriBuilder ->
                    uriBuilder
                        .path(STORAGE_LOCATIONS_PATH)
                        .queryParam("tableUuid", tableUuid)
                        .build())
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<List<Map<?, ?>>>() {})
            .block();
    if (response == null) {
      return List.of();
    }
    return response.stream().map(this::toDto).collect(Collectors.toList());
  }

  @Override
  public void addStorageLocationToTable(String tableUuid, String storageLocationId) {
    htsWebClient
        .post()
        .uri(
            uriBuilder ->
                uriBuilder
                    .path(STORAGE_LOCATIONS_PATH + "/link")
                    .queryParam("tableUuid", tableUuid)
                    .queryParam("storageLocationId", storageLocationId)
                    .build())
        .retrieve()
        .toBodilessEntity()
        .block();
  }

  private StorageLocationDto toDto(Map<?, ?> map) {
    if (map == null) {
      return null;
    }
    return StorageLocationDto.builder()
        .storageLocationId((String) map.get("storageLocationId"))
        .uri((String) map.get("uri"))
        .build();
  }
}
