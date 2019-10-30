package bio.terra.service.iam.exception;

import bio.terra.common.exception.BadRequestException;

public class SamBadRequestException extends BadRequestException {
    public SamBadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamBadRequestException(Throwable cause) {
        super(cause);
    }
}
