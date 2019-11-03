package bio.terra.service.iam.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class IamInternalServerErrorException extends InternalServerErrorException {
    public IamInternalServerErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public IamInternalServerErrorException(Throwable cause) {
        super(cause);
    }

    public IamInternalServerErrorException(String message) {
        super(message);
    }

}
