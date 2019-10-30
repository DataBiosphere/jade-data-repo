package bio.terra.stairway.exception;

/**
 * DatabaseOperationException indicates that something bad happened accessing the database.
 * This might be retry-able.
 */
public class DatabaseOperationException extends StairwayException {
    public DatabaseOperationException(String message) {
        super(message);
    }

    public DatabaseOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseOperationException(Throwable cause) {
        super(cause);
    }
}
