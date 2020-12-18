package scripts.utils.tdrwrapper.exception;

import java.util.List;

public abstract class DataRepoClientException extends RuntimeException {
  private final int statusCode;
  private final List<String> errorDetails;

  public DataRepoClientException(String message, int statusCode, List<String> errorDetails) {
    super(message);
    this.statusCode = statusCode;
    this.errorDetails = errorDetails;
  }

  public DataRepoClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
    this.errorDetails = errorDetails;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public List<String> getErrorDetails() {
    return errorDetails;
  }

  @Override
  public String toString() {
    return String.format(
        "%s (%d) %s",
        super.toString(),
        statusCode,
        !errorDetails.isEmpty()
            ? String.format(" Details: %s", String.join("; ", errorDetails))
            : "");
  }
}
