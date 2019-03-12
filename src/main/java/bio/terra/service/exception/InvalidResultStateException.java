package bio.terra.service.exception;

import bio.terra.exception.InternalServerErrorException;

public class InvalidResultStateException extends InternalServerErrorException {
    public InvalidResultStateException(String message) {
        super(message);
    }

    public InvalidResultStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidResultStateException(Throwable cause) {
        super(cause);
    }
}
