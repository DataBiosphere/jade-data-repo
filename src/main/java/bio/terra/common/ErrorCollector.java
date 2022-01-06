package bio.terra.common;

import bio.terra.service.dataset.exception.IngestFailureException;
import java.util.ArrayList;
import java.util.List;

public class ErrorCollector {
  private List<String> errors;
  private int maxBadLoadFileLineErrorsReported;
  private String exceptionMessage;

  public ErrorCollector(int maxBadLoadFileLineErrorsReported, String exceptionMessage) {
    this.errors = new ArrayList<>();
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
    this.exceptionMessage = exceptionMessage;
  }

  public boolean anyErrorsCollected() {
    return errors.size() > 0;
  }

  public void record(String lineErrorMsgFormat, String lineErrorMsg) {
    if (errors.size() < maxBadLoadFileLineErrorsReported) {
      errors.add(String.format(lineErrorMsgFormat, lineErrorMsg));
    } else {
      errors.add(
          "Error details truncated. [MaxBadLoadFileLineErrorsReported = "
              + maxBadLoadFileLineErrorsReported
              + "]");
      throw getFormattedException();
    }
  }

  public IngestFailureException getFormattedException() {
    return new IngestFailureException(exceptionMessage, errors);
  }
}
