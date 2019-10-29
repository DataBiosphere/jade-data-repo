package bio.terra.stairway.exception;

public abstract class StairwayRuntimeException extends RuntimeException {
    public StairwayRuntimeException(String message) {
        super(message);
    }

    public StairwayRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public StairwayRuntimeException(Throwable cause) {
        super(cause);
    }
}
