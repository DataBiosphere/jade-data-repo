package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidUriException extends BadRequestException {
  public InvalidUriException(String uri) {
    super(formatMessage(uri));
  }

  public InvalidUriException(String uri, Throwable cause) {
    super(formatMessage(uri), cause);
  }

  public InvalidUriException(Throwable cause, String uri) {
    super(cause);
  }

  private static String formatMessage(String uri) {
    return "Invalid URI: %s".formatted(uri);
  }
}
