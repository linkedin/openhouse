from __future__ import annotations

from pyiceberg.exceptions import NoSuchTableError


class OpenHouseCatalogError(Exception):
    """Base error for OpenHouse catalog operations.

    Args:
        message: Human-readable error detail.
        request_id: ID assigned to the associated HTTP request, when available.
    """

    retryable = False

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        self.request_id = request_id
        if request_id is not None:
            message = f"{message}. X-Request-ID: {request_id}"
        super().__init__(message)


class OpenHouseRequestError(OpenHouseCatalogError):
    """Base error for failures while making an OpenHouse HTTP request."""


class OpenHouseTransportError(OpenHouseRequestError):
    """Error raised when an OpenHouse HTTP request fails at the transport layer."""

    retryable = True


class OpenHouseHTTPError(OpenHouseRequestError):
    """Error raised when OpenHouse returns a non-success HTTP response."""

    def __init__(
        self,
        database: str,
        table: str,
        *,
        status_code: int,
        response_body: str,
        request_id: str,
    ) -> None:
        self.status_code = status_code
        self.response_body = response_body
        self.retryable = status_code in {408, 429} or status_code >= 500
        super().__init__(
            f"Failed to load table {database}.{table}: HTTP {status_code}. Response: {response_body}",
            request_id=request_id,
        )

    @classmethod
    def for_response(
        cls,
        database: str,
        table: str,
        *,
        status_code: int,
        response_body: str,
        request_id: str,
    ) -> OpenHouseHTTPError:
        """Build the appropriate typed error for an HTTP response."""
        error_type: type[OpenHouseHTTPError]
        if status_code == 401:
            error_type = OpenHouseAuthenticationError
        elif status_code == 403:
            error_type = OpenHouseAuthorizationError
        elif status_code == 404:
            error_type = OpenHouseNoSuchTableError
        else:
            error_type = cls
        return error_type(
            database,
            table,
            status_code=status_code,
            response_body=response_body,
            request_id=request_id,
        )


class OpenHouseAuthenticationError(OpenHouseHTTPError):
    """Error raised when OpenHouse rejects request authentication (HTTP 401)."""


class OpenHouseAuthorizationError(OpenHouseHTTPError):
    """Error raised when OpenHouse denies access to a resource (HTTP 403)."""


class OpenHouseNoSuchTableError(OpenHouseHTTPError, NoSuchTableError):
    """Error raised when an OpenHouse table does not exist (HTTP 404)."""

    def __init__(
        self,
        database: str,
        table: str,
        *,
        status_code: int,
        response_body: str,
        request_id: str,
    ) -> None:
        self.status_code = status_code
        self.response_body = response_body
        self.retryable = False
        OpenHouseCatalogError.__init__(
            self,
            f"Table {database}.{table} does not exist",
            request_id=request_id,
        )


class OpenHouseInvalidResponseError(OpenHouseCatalogError):
    """Error raised when a successful OpenHouse response is malformed."""
