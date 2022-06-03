package bio.terra.service.auth.oauth2.exceptions;

import bio.terra.common.exception.InternalServerErrorException;

public class OauthTokeninfoRetrieveException extends InternalServerErrorException {

  public OauthTokeninfoRetrieveException(String message, Throwable cause) {
    super(message, cause);
  }
}
