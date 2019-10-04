package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class FileSystemCorruptException extends InternalServerErrorException {
    public FileSystemCorruptException(String message) {
        super(message);
    }

    public FileSystemCorruptException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemCorruptException(Throwable cause) {
        super(cause);
    }
}
