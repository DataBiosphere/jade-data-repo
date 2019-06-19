package bio.terra.dao.exception;

import bio.terra.exception.BadRequestException;

import java.util.List;

public class InvalidStudyException extends BadRequestException {
    public InvalidStudyException(String message) {
        super(message);
    }

    public InvalidStudyException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidStudyException(Throwable cause) {
        super(cause);
    }

    public InvalidStudyException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
