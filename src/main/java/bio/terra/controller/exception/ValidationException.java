package bio.terra.controller.exception;

import bio.terra.exception.BadRequestException;

import java.util.List;

public class ValidationException extends BadRequestException {
    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, List<String> errors) {
        super(message, errors);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidationException(String message, Throwable cause, List<String> errors) {
        super(message, cause, errors);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }
}
