package bio.terra.service.job.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class JobExecutionException extends InternalServerErrorException {
  public JobExecutionException(String message) {
    super(message);
  }

  public JobExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobExecutionException(Throwable cause) {
    super(cause);
  }

  public JobExecutionException(String message, List<String> causes) {
    super(message, causes);
  }
}
