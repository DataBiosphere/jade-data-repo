package bio.terra.service.dataset.flight.update;

import bio.terra.common.exception.ErrorReportException;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public class DatasetSchemaUpdateException extends ErrorReportException {

  public DatasetSchemaUpdateException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatasetSchemaUpdateException(String message, @Nullable List<String> causes) {
    super(message, causes, null);
  }
}
