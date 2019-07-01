package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class DrDatasetNotFoundException extends NotFoundException {
    public DrDatasetNotFoundException(String message) {
        super(message);
    }

    public DrDatasetNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DrDatasetNotFoundException(Throwable cause) {
        super(cause);
    }
}
