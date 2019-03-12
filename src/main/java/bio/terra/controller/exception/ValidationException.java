package bio.terra.controller.exception;

import bio.terra.exception.BadRequestException;

public class ValidationException extends BadRequestException {
    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }
}
