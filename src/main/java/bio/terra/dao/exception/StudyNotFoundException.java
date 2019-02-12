package bio.terra.dao.exception;

public class StudyNotFoundException extends RepositoryMetadataException {
    public StudyNotFoundException(String message) {
        super(message);
    }

    public StudyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public StudyNotFoundException(Throwable cause) {
        super(cause);
    }
}
