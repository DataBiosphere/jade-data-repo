package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoUnauthorizedClientException extends DataRepoClientException {

  public DataRepoUnauthorizedClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoUnauthorizedClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
