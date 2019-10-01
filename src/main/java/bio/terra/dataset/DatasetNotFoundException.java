package bio.terra.dataset;

import bio.terra.exception.NotFoundException;

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
