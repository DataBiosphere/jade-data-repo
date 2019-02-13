package bio.terra.dao.exception;

public class RepositoryMetadataException extends RuntimeException {
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
