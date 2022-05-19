package bio.terra.common.exception;

import java.util.List;

public class PdaoFileCopyException extends BadRequestException {
  public PdaoFileCopyException(String message, Throwable cause) {
    super(message, cause);
  }

  public PdaoFileCopyException(String message, List<String> causes) {
    super(message, causes);
  }
}
