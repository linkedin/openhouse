package com.linkedin.openhouse.tables.exception;

import org.springframework.web.bind.annotation.ExceptionHandler;

public class ViewExceptionHandlerShape {
  @ExceptionHandler(RuntimeException.class)
  public void handle() {}

  public void helper() {}
}
