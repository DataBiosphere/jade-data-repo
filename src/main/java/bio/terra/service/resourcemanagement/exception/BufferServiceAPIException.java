package bio.terra.service.resourcemanagement.exception;

import bio.terra.buffer.client.ApiException;
import bio.terra.common.exception.ErrorReportException;
import java.util.Collections;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Buffer Service. */
public class BufferServiceAPIException extends ErrorReportException {
  private final ApiException apiException;

  public BufferServiceAPIException(ApiException bufferException) {
    super(
        "Error from Buffer Service",
        bufferException,
        Collections.singletonList(bufferException.getResponseBody()),
        HttpStatus.resolve(bufferException.getCode()));
    this.apiException = bufferException;
  }

  // Used to reconstruct error message when reading job result from stairway
  public BufferServiceAPIException(String message) {
    super(message);
    this.apiException = null;
  }

  /** Get the HTTP status code of the underlying response from Buffer Service. */
  public int getApiExceptionStatus() {
    return apiException.getCode();
  }
}
