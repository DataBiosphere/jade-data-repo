package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;
import java.util.List;

public class BulkLoadControlFileException extends BadRequestException {
  public BulkLoadControlFileException(String message) {
    super(message);
  }

  public BulkLoadControlFileException(String message, Throwable cause) {
    super(message, cause);
  }

  public BulkLoadControlFileException(Throwable cause) {
    super(cause);
  }

  public BulkLoadControlFileException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
