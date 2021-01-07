package scripts.utils.tdrwrapper.exception;

import java.util.List;

public class DataRepoBadRequestClientException extends DataRepoClientException {

  public DataRepoBadRequestClientException(
      String message, int statusCode, List<String> errorDetails) {
    super(message, statusCode, errorDetails);
  }

  public DataRepoBadRequestClientException(
      String message, int statusCode, List<String> errorDetails, Throwable cause) {
    super(message, statusCode, errorDetails, cause);
  }
}
