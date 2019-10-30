package bio.terra.service.iam.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class SamInternalServerErrorException extends InternalServerErrorException {
    public SamInternalServerErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamInternalServerErrorException(Throwable cause) {
        super(cause);
    }
}
