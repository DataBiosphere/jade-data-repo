package bio.terra.utils.dao.exception;

import bio.terra.exception.BadRequestException;

public class FileSystemObjectAlreadyExistsException extends BadRequestException {
    public FileSystemObjectAlreadyExistsException(String message) {
        super(message);
    }

    public FileSystemObjectAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemObjectAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
