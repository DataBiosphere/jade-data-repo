package bio.terra.service.job.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class StepExecutionException extends InternalServerErrorException {
  public StepExecutionException(String message) {
    super(message);
  }

  public StepExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public StepExecutionException(Throwable cause) {
    super(cause);
  }

  public StepExecutionException(String message, List<String> causes) {
    super(message, causes);
  }
}
