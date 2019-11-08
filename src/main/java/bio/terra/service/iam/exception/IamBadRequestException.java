package bio.terra.service.iam.exception;

import bio.terra.common.exception.BadRequestException;

public class IamBadRequestException extends BadRequestException {
    public IamBadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public IamBadRequestException(Throwable cause) {
        super(cause);
    }
}
