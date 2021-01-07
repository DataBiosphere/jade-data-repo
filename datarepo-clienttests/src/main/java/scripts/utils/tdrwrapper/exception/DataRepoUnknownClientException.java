package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoUnknownClientException extends DataRepoClientException {

  public DataRepoUnknownClientException(String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoUnknownClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
