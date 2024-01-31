package bio.terra.service.dataset.flight.update;

import bio.terra.common.exception.ErrorReportException;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.springframework.http.HttpStatus;

public class DatasetSchemaUpdateException extends ErrorReportException {

  public DatasetSchemaUpdateException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatasetSchemaUpdateException(String message, @Nullable List<String> causes) {
    super(message, causes, null);
  }

  public DatasetSchemaUpdateException(String message, List<String> causes, HttpStatus status) {
    super(message, causes, status);
  }
}
