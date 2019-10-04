package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

import java.util.List;

public class IngestFailureException extends BadRequestException {
    public IngestFailureException(String message) {
        super(message);
    }

    public IngestFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public IngestFailureException(Throwable cause) {
        super(cause);
    }

    public IngestFailureException(String message, List<String> errorDetail) {
        super(message, errorDetail);
    }
}
