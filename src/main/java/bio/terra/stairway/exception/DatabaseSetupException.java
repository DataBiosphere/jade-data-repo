package bio.terra.stairway.exception;

/**
 * DatabaseSetupException indicates that something is wrong with the database environment.
 * We were not able to create or reset or recover the database. This is fatal for stairway.
 */
public class DatabaseSetupException extends StairwayException {
    public DatabaseSetupException(String message) {
        super(message);
    }

    public DatabaseSetupException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseSetupException(Throwable cause) {
        super(cause);
    }
}
