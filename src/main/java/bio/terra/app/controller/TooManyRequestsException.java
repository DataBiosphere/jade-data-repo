package bio.terra.app.controller;

/**
 * This exception maps to HttpStatus.TOO_MANY_REQUESTS in the GlobalExceptionHandler.
 */
public class TooManyRequestsException extends Throwable {
    public TooManyRequestsException(String message) {
        super(message);
    }
}
