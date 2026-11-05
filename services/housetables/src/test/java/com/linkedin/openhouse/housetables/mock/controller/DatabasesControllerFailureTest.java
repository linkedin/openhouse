package com.linkedin.openhouse.housetables.mock.controller;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.housetables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.housetables.controller.DatabasesController;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class DatabasesControllerFailureTest {
  @ParameterizedTest
  @CsvSource({"CONCURRENT_MODIFICATION,409", "UNAVAILABLE,503", "STORAGE_FAILURE,500"})
  void testCheckedFailuresHaveExplicitHttpStatuses(
      DatabaseMetadataException.Reason reason, int expectedStatus) throws Exception {
    DatabasesApiHandler handler = mock(DatabasesApiHandler.class);
    when(handler.getDatabase("foo")).thenThrow(new DatabaseMetadataException(reason, "message"));
    MockMvcBuilders.standaloneSetup(new DatabasesController(handler))
        .setMessageConverters(
            new org.springframework.http.converter.json.MappingJackson2HttpMessageConverter())
        .build()
        .perform(get("/hts/databases/foo"))
        .andExpect(status().is(expectedStatus))
        .andExpect(jsonPath("$.message").value("message"));
  }
}
