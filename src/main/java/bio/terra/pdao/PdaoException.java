package bio.terra.pdao;

/**
 * StairwayExecutionException indicates that something is wrong in the Stairway execution code; an invalid state
 * or similar.
 */
public class PdaoException extends RuntimeException {
    public PdaoException(String message) {
        super(message);
    }

    public PdaoException(String message, Throwable cause) {
        super(message, cause);
    }

    public PdaoException(Throwable cause) {
        super(cause);
    }
}
