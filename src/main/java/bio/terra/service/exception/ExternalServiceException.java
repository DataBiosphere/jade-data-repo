package bio.terra.service.exception;

import bio.terra.exception.InternalServerErrorException;

public class ExternalServiceException extends InternalServerErrorException {
    public ExternalServiceException(String message) {
        super(message);
    }

    public ExternalServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalServiceException(Throwable cause) {
        super(cause);
    }
}
