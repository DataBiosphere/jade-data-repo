package bio.terra.filesystem.exception;

import bio.terra.exception.BadRequestException;

public class FileAlreadyExistsException extends BadRequestException {
    public FileAlreadyExistsException(String message) {
        super(message);
    }

    public FileAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
