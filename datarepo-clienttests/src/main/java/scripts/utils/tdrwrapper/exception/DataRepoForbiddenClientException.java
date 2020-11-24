package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoForbiddenClientException extends DataRepoClientException {

  public DataRepoForbiddenClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoForbiddenClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
