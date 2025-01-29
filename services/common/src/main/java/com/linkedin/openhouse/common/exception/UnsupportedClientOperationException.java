package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating an operation is not supported
 *
 * <p>message contains the details of the operation which is not supported. This error is mapped to
 * 400 server error. Use this error to only indicate an invalid operation done by the client (for
 * example: partition evolution). Avoid using this exception for internal server errors.
 */
public class UnsupportedClientOperationException extends RuntimeException {

  public UnsupportedClientOperationException(Operation operation, String message) {
    super(message);
  }

  public enum Operation {
    PARTITION_EVOLUTION,
    ALTER_RESERVED_TBLPROPS,
    ALTER_RESERVED_ROLES,
    GRANT_ON_UNSHARED_TABLES,
    ALTER_TABLE_TYPE
  }
}
