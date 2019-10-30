package bio.terra.stairway.exception;

public class FlightException extends StairwayException {
    public FlightException(String message) {
        super(message);
    }

    public FlightException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlightException(Throwable cause) {
        super(cause);
    }
}
