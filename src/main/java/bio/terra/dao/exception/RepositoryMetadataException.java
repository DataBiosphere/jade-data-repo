package bio.terra.dao.exception;

public class RepositoryMetadataException extends Exception {
    public RepositoryMetadataException(String message) {
        super(message);
    }

    public RepositoryMetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryMetadataException(Throwable cause) {
        super(cause);
    }
}
