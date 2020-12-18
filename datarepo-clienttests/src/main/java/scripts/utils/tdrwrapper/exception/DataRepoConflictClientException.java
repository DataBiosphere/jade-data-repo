package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoConflictClientException extends DataRepoClientException {

  public DataRepoConflictClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoConflictClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
