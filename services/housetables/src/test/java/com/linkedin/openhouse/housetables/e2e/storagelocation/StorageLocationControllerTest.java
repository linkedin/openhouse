package com.linkedin.openhouse.housetables.e2e.storagelocation;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.StorageLocation;
import com.linkedin.openhouse.housetables.model.StorageLocationRow;
import com.linkedin.openhouse.housetables.model.TableStorageLocationRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.StorageLocationJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.TableStorageLocationJdbcRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@AutoConfigureMockMvc
public class StorageLocationControllerTest {

  @Autowired MockMvc mvc;

  @Autowired StorageLocationJdbcRepository storageLocationRepo;

  @Autowired TableStorageLocationJdbcRepository tableStorageLocationRepo;

  @AfterEach
  public void tearDown() {
    tableStorageLocationRepo.deleteAll();
    storageLocationRepo.deleteAll();
  }

  @Test
  public void testCreateStorageLocation() throws Exception {
    StorageLocation request = StorageLocation.builder().uri("hdfs://nn/data/loc1").build();
    mvc.perform(
            MockMvcRequestBuilders.post("/hts/storageLocations")
                .contentType(MediaType.APPLICATION_JSON)
                .content(request.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.storageLocationId", not(emptyOrNullString())))
        .andExpect(jsonPath("$.uri", is(equalTo("hdfs://nn/data/loc1"))));
  }

  @Test
  public void testGetStorageLocationById() throws Exception {
    StorageLocationRow saved =
        storageLocationRepo.save(
            StorageLocationRow.builder()
                .storageLocationId("sl-1")
                .uri("hdfs://nn/data/loc1")
                .build());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/storageLocations/" + saved.getStorageLocationId())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.storageLocationId", is(equalTo("sl-1"))))
        .andExpect(jsonPath("$.uri", is(equalTo("hdfs://nn/data/loc1"))));
  }

  @Test
  public void testGetStorageLocationNotFound() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/storageLocations/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))));
  }

  @Test
  public void testGetLocationsForTable() throws Exception {
    storageLocationRepo.save(
        StorageLocationRow.builder().storageLocationId("sl-a").uri("hdfs://nn/data/a").build());
    storageLocationRepo.save(
        StorageLocationRow.builder().storageLocationId("sl-b").uri("hdfs://nn/data/b").build());
    tableStorageLocationRepo.save(
        TableStorageLocationRow.builder()
            .databaseId("db1")
            .tableId("tb1")
            .storageLocationId("sl-a")
            .build());
    tableStorageLocationRepo.save(
        TableStorageLocationRow.builder()
            .databaseId("db1")
            .tableId("tb1")
            .storageLocationId("sl-b")
            .build());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/storageLocations")
                .param("databaseId", "db1")
                .param("tableId", "tb1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$", hasSize(2)));
  }

  @Test
  public void testGetLocationsForTableEmpty() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/storageLocations")
                .param("databaseId", "db1")
                .param("tableId", "none")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$", hasSize(0)));
  }

  @Test
  public void testLinkStorageLocation() throws Exception {
    storageLocationRepo.save(
        StorageLocationRow.builder()
            .storageLocationId("sl-link")
            .uri("hdfs://nn/data/link")
            .build());

    mvc.perform(
            MockMvcRequestBuilders.post("/hts/storageLocations/link")
                .param("databaseId", "db1")
                .param("tableId", "tb1")
                .param("storageLocationId", "sl-link"))
        .andExpect(status().isNoContent());

    // Verify the link was persisted
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/storageLocations")
                .param("databaseId", "db1")
                .param("tableId", "tb1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$", hasSize(1)))
        .andExpect(jsonPath("$[0].storageLocationId", is(equalTo("sl-link"))));
  }

  @Test
  public void testLinkNonexistentLocation() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post("/hts/storageLocations/link")
                .param("databaseId", "db1")
                .param("tableId", "tb1")
                .param("storageLocationId", "bad"))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testCreateMissingUri() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post("/hts/storageLocations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }
}
