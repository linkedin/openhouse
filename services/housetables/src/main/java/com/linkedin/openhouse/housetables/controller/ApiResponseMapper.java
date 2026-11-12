package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import org.springframework.http.ResponseEntity;

final class ApiResponseMapper {
  private ApiResponseMapper() {}

  static <T> ResponseEntity<T> toResponseEntity(ApiResponse<T> response) {
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }
}
