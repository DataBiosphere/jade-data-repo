package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoNotImplementedClientException extends DataRepoClientException {

  public DataRepoNotImplementedClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoNotImplementedClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
