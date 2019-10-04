package bio.terra.service.dataset.exception;

import bio.terra.common.exception.NotFoundException;

public class TableNotFoundException extends NotFoundException {
    public TableNotFoundException(String message) {
        super(message);
    }

    public TableNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public TableNotFoundException(Throwable cause) {
        super(cause);
    }
}
