package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class InvalidFileSystemObjectTypeException extends NotFoundException {
    public InvalidFileSystemObjectTypeException(String message) {
        super(message);
    }

    public InvalidFileSystemObjectTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidFileSystemObjectTypeException(Throwable cause) {
        super(cause);
    }
}
