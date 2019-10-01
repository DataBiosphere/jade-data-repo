package bio.terra.filedata.exception;

import bio.terra.exception.NotFoundException;

public class FileNotFoundException extends NotFoundException {
    public FileNotFoundException(String message) {
        super(message);
    }

    public FileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileNotFoundException(Throwable cause) {
        super(cause);
    }
}
