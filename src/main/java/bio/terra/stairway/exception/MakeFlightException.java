package bio.terra.stairway.exception;

public class MakeFlightException extends StairwayRuntimeException {
    public MakeFlightException(String message) {
        super(message);
    }

    public MakeFlightException(String message, Throwable cause) {
        super(message, cause);
    }

    public MakeFlightException(Throwable cause) {
        super(cause);
    }
}
