package com.linkedin.openhouse.housetables.mock.controller;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.controller.NamespacesController;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class NamespacesControllerFailureTest {
  @ParameterizedTest
  @CsvSource({"CONCURRENT_MODIFICATION,409", "UNAVAILABLE,503", "STORAGE_FAILURE,500"})
  void testCheckedFailuresHaveExplicitHttpStatuses(
      NamespaceMetadataException.Reason reason, int expectedStatus) throws Exception {
    NamespacesApiHandler handler = mock(NamespacesApiHandler.class);
    when(handler.getNamespace("foo")).thenThrow(new NamespaceMetadataException(reason, "message"));
    MockMvcBuilders.standaloneSetup(new NamespacesController(handler))
        .setMessageConverters(new MappingJackson2HttpMessageConverter())
        .build()
        .perform(get("/hts/namespaces/foo"))
        .andExpect(status().is(expectedStatus))
        .andExpect(jsonPath("$.message").value("message"));
  }
}
