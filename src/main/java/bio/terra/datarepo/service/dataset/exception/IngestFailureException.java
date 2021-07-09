package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.BadRequestException;
import java.util.List;

public class IngestFailureException extends BadRequestException {
  public IngestFailureException(String message) {
    super(message);
  }

  public IngestFailureException(String message, Throwable cause) {
    super(message, cause);
  }

  public IngestFailureException(Throwable cause) {
    super(cause);
  }

  public IngestFailureException(String message, List<String> errorDetail) {
    super(message, errorDetail);
  }
}
