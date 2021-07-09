package bio.terra.datarepo.service.search.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class SearchException extends InternalServerErrorException {
  public SearchException(String message) {
    super(message);
  }

  public SearchException(String message, Throwable cause) {
    super(message, cause);
  }

  public SearchException(Throwable cause) {
    super(cause);
  }
}
