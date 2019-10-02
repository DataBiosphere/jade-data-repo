package bio.terra.filedata;

import bio.terra.common.exception.NotFoundException;

public class FileSystemObjectNotFoundException extends NotFoundException {
    public FileSystemObjectNotFoundException(String message) {
        super(message);
    }

    public FileSystemObjectNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemObjectNotFoundException(Throwable cause) {
        super(cause);
    }
}
