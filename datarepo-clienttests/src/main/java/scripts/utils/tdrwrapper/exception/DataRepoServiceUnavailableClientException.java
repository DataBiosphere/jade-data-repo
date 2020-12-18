package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoServiceUnavailableClientException extends DataRepoClientException {

  public DataRepoServiceUnavailableClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoServiceUnavailableClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
