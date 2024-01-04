package bio.terra.common.exception;

import java.util.List;

public class FeatureNotImplementedException extends NotImplementedException {

  public FeatureNotImplementedException(String message) {
    super(message);
  }

  public FeatureNotImplementedException(String message, Throwable cause) {
    super(message, cause);
  }

  public FeatureNotImplementedException(Throwable cause) {
    super(cause);
  }

  public FeatureNotImplementedException(String message, List<String> causes) {
    super(message, causes);
  }

  public FeatureNotImplementedException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes);
  }
}
