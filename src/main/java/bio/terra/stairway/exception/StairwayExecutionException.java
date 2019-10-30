package bio.terra.stairway.exception;

/**
 * StairwayExecutionException indicates that something is wrong in the Stairway execution code; an invalid state
 * or similar.
 */
public class StairwayExecutionException extends StairwayException {
    public StairwayExecutionException(String message) {
        super(message);
    }

    public StairwayExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public StairwayExecutionException(Throwable cause) {
        super(cause);
    }
}
