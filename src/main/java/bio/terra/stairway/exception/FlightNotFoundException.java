package bio.terra.stairway.exception;

public class FlightNotFoundException extends RuntimeException {
    public FlightNotFoundException(String message) {
        super(message);
    }

    public FlightNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlightNotFoundException(Throwable cause) {
        super(cause);
    }
}
