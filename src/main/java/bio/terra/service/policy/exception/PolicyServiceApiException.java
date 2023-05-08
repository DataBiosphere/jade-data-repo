package bio.terra.service.policy.exception;

import bio.terra.buffer.client.ApiException;
import bio.terra.common.exception.ErrorReportException;
import java.util.Collections;
import java.util.List;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Terra Policy Service. */
public class PolicyServiceApiException extends ErrorReportException {
  private ApiException apiException;

  public PolicyServiceApiException(ApiException ex) {
    super(
        "Error from Policy Service: ",
        ex,
        Collections.singletonList(ex.getResponseBody()),
        HttpStatus.resolve(ex.getCode()));
    this.apiException = ex;
  }

  public PolicyServiceApiException(String message) {
    super(message);
  }

  public PolicyServiceApiException(String message, Throwable cause) {
    super(message, cause);
  }

  public PolicyServiceApiException(Throwable cause) {
    super(cause);
  }

  public PolicyServiceApiException(String message, List<String> causes, HttpStatus statusCode) {
    super(message, causes, statusCode);
  }

  public PolicyServiceApiException(
      String message, Throwable cause, List<String> causes, HttpStatus statusCode) {
    super(message, cause, causes, statusCode);
  }

  /** Get the HTTP status code of the underlying response from Policy Service. */
  public int getApiExceptionStatus() {
    return apiException.getCode();
  }
}
