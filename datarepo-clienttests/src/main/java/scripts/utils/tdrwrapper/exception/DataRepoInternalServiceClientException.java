package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoInternalServiceClientException extends DataRepoClientException {

  public DataRepoInternalServiceClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoInternalServiceClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
