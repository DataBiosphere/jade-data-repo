package bio.terra.upgrade;

/**
 * Migrate exception is thrown on Liquibase or SQL exceptions during migration.
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
