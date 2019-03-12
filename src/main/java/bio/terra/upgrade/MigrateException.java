package bio.terra.upgrade;

/**
 * Migrate exception is thrown on Liquibase or SQL exception during migration.
 */
public class MigrateException extends RuntimeException {
    public MigrateException(String message) {
        super(message);
    }

    public MigrateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MigrateException(Throwable cause) {
        super(cause);
    }
}
