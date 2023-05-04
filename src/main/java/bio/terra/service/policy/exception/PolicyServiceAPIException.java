package bio.terra.service.policy.exception;

import bio.terra.buffer.client.ApiException;
import bio.terra.common.exception.ErrorReportException;
import java.util.Collections;
import java.util.List;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Terra Policy Service. */
public class PolicyServiceAPIException extends ErrorReportException {
  private ApiException apiException;

  public PolicyServiceAPIException(ApiException ex) {
    super(
        "Error from Policy Service: ",
        ex,
        Collections.singletonList(ex.getResponseBody()),
        HttpStatus.resolve(ex.getCode()));
    this.apiException = ex;
  }

  public PolicyServiceAPIException(String message) {
    super(message);
  }

  public PolicyServiceAPIException(String message, Throwable cause) {
    super(message, cause);
  }

  public PolicyServiceAPIException(Throwable cause) {
    super(cause);
  }

  public PolicyServiceAPIException(String message, List<String> causes, HttpStatus statusCode) {
    super(message, causes, statusCode);
  }

  public PolicyServiceAPIException(
      String message, Throwable cause, List<String> causes, HttpStatus statusCode) {
    super(message, cause, causes, statusCode);
  }

  /** Get the HTTP status code of the underlying response from Policy Service. */
  public int getApiExceptionStatus() {
    return apiException.getCode();
  }
}
