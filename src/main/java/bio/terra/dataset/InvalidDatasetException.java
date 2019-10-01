package bio.terra.dataset;

import bio.terra.exception.BadRequestException;

import java.util.List;

public class InvalidDatasetException extends BadRequestException {
    public InvalidDatasetException(String message) {
        super(message);
    }

    public InvalidDatasetException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidDatasetException(Throwable cause) {
        super(cause);
    }

    public InvalidDatasetException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
