package bio.terra.utils.dao.exception;

import bio.terra.exception.BadRequestException;

public class FileSystemObjectDependencyException extends BadRequestException {
    public FileSystemObjectDependencyException(String message) {
        super(message);
    }

    public FileSystemObjectDependencyException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemObjectDependencyException(Throwable cause) {
        super(cause);
    }
}
