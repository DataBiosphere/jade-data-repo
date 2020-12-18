package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoNotFoundClientException extends DataRepoClientException {

  public DataRepoNotFoundClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoNotFoundClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
