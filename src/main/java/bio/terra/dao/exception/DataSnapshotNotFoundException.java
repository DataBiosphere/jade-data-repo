package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class DataSnapshotNotFoundException extends NotFoundException {
    public DataSnapshotNotFoundException(String message) {
        super(message);
    }

    public DataSnapshotNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSnapshotNotFoundException(Throwable cause) {
        super(cause);
    }
}
