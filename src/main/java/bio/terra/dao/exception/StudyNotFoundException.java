package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class StudyNotFoundException extends NotFoundException {
    public StudyNotFoundException(String message) {
        super(message);
    }

    public StudyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public StudyNotFoundException(Throwable cause) {
        super(cause);
    }
}
