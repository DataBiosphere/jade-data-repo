package bio.terra.common;

import bio.terra.common.exception.BadRequestException;
import java.util.ArrayList;
import java.util.List;

public class ErrorCollector {
  private final List<String> errors;
  private final int maxErrorsReported;
  private final String exceptionMessage;

  public ErrorCollector(int maxErrorsReported, String exceptionMessage) {
    this.errors = new ArrayList<>();
    this.maxErrorsReported = maxErrorsReported;
    this.exceptionMessage = exceptionMessage;
  }

  public boolean anyErrorsCollected() {
    return errors.size() > 0;
  }

  public void record(String lineErrorMsgFormat, Object... lineErrorMsgArgs) {
    synchronized (errors) {
      if (errors.size() < maxErrorsReported) {
        errors.add(String.format(lineErrorMsgFormat, lineErrorMsgArgs));
      } else if (errors.size() == maxErrorsReported) {
        errors.add(
            "Error details truncated. [MaxBadLoadFileLineErrorsReported = "
                + maxErrorsReported
                + "]");
        throw getFormattedException();
      }
    }
  }

  public BadRequestException getFormattedException() {
    return new BadRequestException(exceptionMessage, errors);
  }
}
