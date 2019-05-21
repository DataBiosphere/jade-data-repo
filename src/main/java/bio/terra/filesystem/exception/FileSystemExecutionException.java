package bio.terra.filesystem.exception;

import bio.terra.exception.InternalServerErrorException;

public class FileSystemExecutionException extends InternalServerErrorException {
    public FileSystemExecutionException(String message) {
        super(message);
    }

    public FileSystemExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemExecutionException(Throwable cause) {
        super(cause);
    }
}
