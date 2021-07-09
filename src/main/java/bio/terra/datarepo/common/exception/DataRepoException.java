package bio.terra.datarepo.common.exception;

import java.util.Collections;
import java.util.List;

/**
 * DataRepoException is the base exception for the other data repository exceptions. It adds a list
 * of strings to provide error details. These are used in several cases. For example,
 *
 * <ul>
 *   <li>validation errors - to return details of each check that failed
 *   <li>id mismatch errors - invalid file id and row id references
 * </ul>
 *
 * Each subclass of this exception maps to a single HTTP status code.
 */
public abstract class DataRepoException extends RuntimeException {
  private final List<String> errorDetails;

  public DataRepoException(String message) {
    super(message);
    this.errorDetails = Collections.emptyList();
  }

  public DataRepoException(String message, Throwable cause) {
    super(message, cause);
    this.errorDetails = Collections.emptyList();
  }

  public DataRepoException(Throwable cause) {
    super(cause);
    this.errorDetails = Collections.emptyList();
  }

  public DataRepoException(String message, List<String> errorDetails) {
    super(message);
    this.errorDetails = errorDetails;
  }

  public DataRepoException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause);
    this.errorDetails = errorDetails;
  }

  public List<String> getErrorDetails() {
    return errorDetails;
  }

  @Override
  public String toString() {
    return String.format(
        "%s%s",
        super.toString(),
        !errorDetails.isEmpty()
            ? String.format(" Details: %s", String.join("; ", errorDetails))
            : "");
  }
}
