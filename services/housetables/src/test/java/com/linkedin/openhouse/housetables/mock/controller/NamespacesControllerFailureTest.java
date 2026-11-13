package com.linkedin.openhouse.housetables.mock.controller;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.AuthenticationRequiredException;
import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.exception.checked.PermissionDeniedException;
import com.linkedin.openhouse.common.exception.checked.ServiceFailureException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.common.exception.checked.VersionConflictException;
import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.controller.NamespacesController;
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
      DomainException exception, int expectedStatus, String expectedType, String expectedMessage)
      throws Exception {
    NamespacesApiHandler handler = mock(NamespacesApiHandler.class);
    when(handler.getNamespace("foo")).thenThrow(exception);
    MockMvcBuilders.standaloneSetup(new NamespacesController(handler))
        .setMessageConverters(new MappingJackson2HttpMessageConverter())
        .build()
        .perform(get("/hts/namespaces/foo"))
        .andExpect(status().is(expectedStatus))
        .andExpect(jsonPath("$.message").value(expectedMessage))
        .andExpect(jsonPath("$.type").value(expectedType))
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        .andExpect(jsonPath("$.cause").doesNotExist());
  }

  static Stream<Arguments> checkedFailures() {
    Throwable cause = new IllegalStateException("internal storage details");
    return Stream.of(
        Arguments.of(
            new NotFoundException("message", cause),
            404,
            "NotFoundException",
            "Namespace metadata does not exist."),
        Arguments.of(
            new AlreadyExistsException("message", cause),
            409,
            "AlreadyExistsException",
            "Namespace name already exists."),
        Arguments.of(
            new InvalidRequestException("message", cause),
            400,
            "InvalidRequestException",
            "message"),
        Arguments.of(
            new InvalidPropertyUpdateException("message", cause),
            422,
            "InvalidPropertyUpdateException",
            "Property updates and removals must have disjoint keys."),
        Arguments.of(
            new VersionConflictException("message", cause),
            409,
            "VersionConflictException",
            "Namespace metadata changed concurrently."),
        Arguments.of(
            new UnavailableException("message", cause),
            503,
            "UnavailableException",
            "Namespace metadata service unavailable."),
        Arguments.of(
            new ServiceFailureException("message", cause),
            500,
            "ServiceFailureException",
            "Namespace metadata operation failed."),
        Arguments.of(
            new MutationUncertainException("message", cause),
            500,
            "MutationUncertainException",
            "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."),
        Arguments.of(
            new AuthenticationRequiredException("message", cause),
            401,
            "AuthenticationRequiredException",
            "Authentication required."),
        Arguments.of(
            new PermissionDeniedException("message", cause),
            403,
            "PermissionDeniedException",
            "Access denied."));
  }

  @Test
  void unexpectedMutationFailureCannotClaimConfirmedRejection() throws Exception {
    NamespacesApiHandler handler = mock(NamespacesApiHandler.class);
    when(handler.createNamespace(any()))
        .thenThrow(new IllegalStateException("secret property value"));
    MockMvcBuilders.standaloneSetup(new NamespacesController(handler))
        .setMessageConverters(new MappingJackson2HttpMessageConverter())
        .build()
        .perform(
            post("/hts/namespaces")
                .contentType("application/json")
                .content("{\"namespaceName\":\"foo\"}"))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.type").value("MutationUncertainException"))
        .andExpect(
            jsonPath("$.message")
                .value(
                    "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."))
        .andExpect(jsonPath("$.cause").doesNotExist());
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
