package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

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
