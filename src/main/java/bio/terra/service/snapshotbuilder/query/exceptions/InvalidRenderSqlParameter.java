package bio.terra.service.snapshotbuilder.query.exceptions;

import bio.terra.common.exception.InternalServerErrorException;

public class InvalidRenderSqlParameter extends InternalServerErrorException {
  public InvalidRenderSqlParameter(String message) {
    super(message);
  }
}
