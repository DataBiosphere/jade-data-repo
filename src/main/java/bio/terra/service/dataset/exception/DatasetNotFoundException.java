package bio.terra.service.dataset.exception;

import bio.terra.common.exception.NotFoundException;

public class DatasetNotFoundException extends NotFoundException {
    public DatasetNotFoundException(String message) {
        super(message);
    }

    public DatasetNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatasetNotFoundException(Throwable cause) {
        super(cause);
    }
}
