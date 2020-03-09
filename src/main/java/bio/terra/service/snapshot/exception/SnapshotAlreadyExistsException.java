package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class SnapshotAlreadyExistsException extends InternalServerErrorException {
    public SnapshotAlreadyExistsException(String message) {
        super(message);
    }

    public SnapshotAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public SnapshotAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
