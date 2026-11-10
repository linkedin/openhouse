package com.linkedin.openhouse.housetables.mock.controller;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.controller.NamespacesController;
import com.linkedin.openhouse.housetables.services.InvalidNamespacePropertyUpdateException;
import com.linkedin.openhouse.housetables.services.InvalidNamespaceRequestException;
import com.linkedin.openhouse.housetables.services.NamespaceAlreadyExistsException;
import com.linkedin.openhouse.housetables.services.NamespaceConcurrentModificationException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataStorageException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataUnavailableException;
import com.linkedin.openhouse.housetables.services.NamespaceNotFoundException;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class NamespacesControllerFailureTest {
  @ParameterizedTest
  @MethodSource("checkedFailures")
  void testCheckedFailuresHaveExplicitHttpStatuses(
      NamespaceMetadataException exception, int expectedStatus) throws Exception {
    NamespacesApiHandler handler = mock(NamespacesApiHandler.class);
    when(handler.getNamespace("foo")).thenThrow(exception);
    MockMvcBuilders.standaloneSetup(new NamespacesController(handler))
        .setMessageConverters(new MappingJackson2HttpMessageConverter())
        .build()
        .perform(get("/hts/namespaces/foo"))
        .andExpect(status().is(expectedStatus))
        .andExpect(jsonPath("$.message").value("message"))
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        .andExpect(jsonPath("$.cause").doesNotExist());
  }

  static Stream<Arguments> checkedFailures() {
    Throwable cause = new IllegalStateException("internal storage details");
    return Stream.of(
        Arguments.of(new NamespaceNotFoundException("message", cause), 404),
        Arguments.of(new NamespaceAlreadyExistsException("message", cause), 409),
        Arguments.of(new InvalidNamespaceRequestException("message", cause), 400),
        Arguments.of(new InvalidNamespacePropertyUpdateException("message", cause), 422),
        Arguments.of(new NamespaceConcurrentModificationException("message", cause), 409),
        Arguments.of(new NamespaceMetadataUnavailableException("message", cause), 503),
        Arguments.of(new NamespaceMetadataStorageException("message", cause), 500));
  }

  @Test
  void testHandlerResponseBodyHeadersAndStatusReachEveryEndpoint() throws Exception {
    NamespacesApiHandler handler = mock(NamespacesApiHandler.class);
    HttpHeaders headers = new HttpHeaders();
    headers.add("X-Namespace", "foo");
    NamespaceMetadataResponse metadata =
        NamespaceMetadataResponse.builder()
            .namespaceUuid("identifier")
            .namespaceName("foo")
            .properties(Collections.singletonMap("bar", "one"))
            .build();
    when(handler.createNamespace(any()))
        .thenReturn(
            ApiResponse.<NamespaceMetadataResponse>builder()
                .responseBody(metadata)
                .httpHeaders(headers)
                .httpStatus(HttpStatus.CREATED)
                .build());
    when(handler.getNamespace("foo"))
        .thenReturn(
            ApiResponse.<NamespaceMetadataResponse>builder()
                .responseBody(metadata)
                .httpHeaders(headers)
                .httpStatus(HttpStatus.ACCEPTED)
                .build());
    when(handler.updateNamespaceProperties(eq("foo"), any()))
        .thenReturn(
            ApiResponse.<UpdateNamespacePropertiesResponseBody>builder()
                .responseBody(
                    UpdateNamespacePropertiesResponseBody.builder()
                        .updated(Collections.singletonList("bar"))
                        .removed(Collections.emptyList())
                        .missing(Collections.emptyList())
                        .build())
                .httpHeaders(headers)
                .httpStatus(HttpStatus.OK)
                .build());
    when(handler.deleteNamespace("foo"))
        .thenReturn(
            ApiResponse.<Void>builder()
                .httpHeaders(headers)
                .httpStatus(HttpStatus.NO_CONTENT)
                .build());
    MockMvc mockMvc =
        MockMvcBuilders.standaloneSetup(new NamespacesController(handler))
            .setMessageConverters(new MappingJackson2HttpMessageConverter())
            .build();
    mockMvc
        .perform(
            post("/hts/namespaces")
                .contentType("application/json")
                .content("{\"namespaceName\":\"foo\",\"properties\":{\"bar\":\"one\"}}"))
        .andExpect(status().isCreated())
        .andExpect(header().string("X-Namespace", "foo"))
        .andExpect(jsonPath("$.namespaceUuid").value("identifier"))
        .andExpect(jsonPath("$.properties.bar").value("one"));
    mockMvc
        .perform(get("/hts/namespaces/foo"))
        .andExpect(status().isAccepted())
        .andExpect(header().string("X-Namespace", "foo"))
        .andExpect(jsonPath("$.namespaceName").value("foo"))
        .andExpect(jsonPath("$.properties.bar").value("one"));
    mockMvc
        .perform(
            post("/hts/namespaces/foo/properties")
                .contentType("application/json")
                .content("{\"updates\":{\"bar\":\"two\"},\"removals\":[]}"))
        .andExpect(status().isOk())
        .andExpect(header().string("X-Namespace", "foo"))
        .andExpect(jsonPath("$.updated[0]").value("bar"));
    mockMvc
        .perform(delete("/hts/namespaces/foo"))
        .andExpect(status().isNoContent())
        .andExpect(header().string("X-Namespace", "foo"))
        .andExpect(content().string(""));
  }
}
