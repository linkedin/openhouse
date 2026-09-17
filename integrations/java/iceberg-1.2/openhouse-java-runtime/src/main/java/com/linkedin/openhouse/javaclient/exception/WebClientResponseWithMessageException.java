package com.linkedin.openhouse.javaclient.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * {@link WebClientResponseException} does not display the server's human-readable error message.
 *
 * <p>This wrapper exposes only the error envelope's message, code and request ID, never its raw
 * diagnostics. It preserves the status code and original exception for diagnostics.
 *
 * <p>An exception thrown in Openhouse clients to indicate an error in a tables API request, acting
 * as a wrapper around WebClientResponseException.
 */
public class WebClientResponseWithMessageException extends WebClientWithMessageException {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final WebClientResponseException responseException;
  private final String responseMessage;

  /**
   * Formats both legacy and extended ErrorResponseBody envelopes without depending on the generated
   * model version. Non-envelope responses use the HTTP status, not arbitrary proxy or server
   * output.
   */
  public static String getResponseMessage(WebClientResponseException exception) {
    int statusCode = exception.getRawStatusCode();
    HttpStatus status = HttpStatus.resolve(statusCode);
    String summary = "HTTP " + statusCode + (status == null ? "" : " " + status.getReasonPhrase());
    String body = exception.getResponseBodyAsString();
    if (body == null || body.trim().isEmpty()) {
      return summary;
    }

    final JsonNode envelope;
    try {
      envelope = MAPPER.readTree(body);
    } catch (JsonProcessingException ignored) {
      return summary;
    }
    if (envelope == null || !envelope.isObject()) {
      return summary;
    }

    String message = textField(envelope, "message");
    String code = textField(envelope, "code");
    String requestId = textField(envelope, "requestId");
    StringBuilder result = new StringBuilder(summary);
    if (message != null) {
      result.append(": ").append(message);
    }
    if (code != null || requestId != null) {
      result.append(" [");
      if (code != null) {
        result.append("code=").append(code);
      }
      if (requestId != null) {
        if (code != null) {
          result.append(", ");
        }
        result.append("requestId=").append(requestId);
      }
      result.append(']');
    }
    return result.toString();
  }

  private static String textField(JsonNode envelope, String name) {
    JsonNode field = envelope.get(name);
    if (field == null || !field.isTextual() || field.textValue().trim().isEmpty()) {
      return null;
    }
    return field.textValue();
  }

  public WebClientResponseWithMessageException(WebClientResponseException exception) {
    this.responseException = exception;
    this.responseMessage = getResponseMessage(exception);
    initCause(exception);
  }

  @Override
  public String getMessage() {
    return responseMessage;
  }

  public int getStatusCode() {
    return responseException.getRawStatusCode();
  }
}
