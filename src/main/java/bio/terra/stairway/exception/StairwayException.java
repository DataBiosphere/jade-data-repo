package bio.terra.stairway.exception;

public abstract class StairwayException extends Exception {
    public StairwayException(String message) {
        super(message);
    }

    public StairwayException(String message, Throwable cause) {
        super(message, cause);
    }

    public StairwayException(Throwable cause) {
        super(cause);
    }
}
