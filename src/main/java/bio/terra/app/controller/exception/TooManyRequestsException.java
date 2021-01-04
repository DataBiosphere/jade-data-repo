package bio.terra.app.controller.exception;

import bio.terra.common.exception.BadRequestException;

import java.util.List;

/**
 * This exception maps to HttpStatus.TOO_MANY_REQUESTS in the GlobalExceptionHandler.
 */
public class TooManyRequestsException extends BadRequestException {
    public TooManyRequestsException(String message) {
        super(message);
    }

    public TooManyRequestsException(String message, List<String> errors) {
        super(message, errors);
    }

    public TooManyRequestsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TooManyRequestsException(String message, Throwable cause, List<String> errors) {
        super(message, cause, errors);
    }

    public TooManyRequestsException(Throwable cause) {
        super(cause);
    }
}
