package bio.terra.service.kubernetes.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class KubeApiException extends InternalServerErrorException {
    public KubeApiException(String message) {
        super(message);
    }

    public KubeApiException(String message, Throwable cause) {
        super(message, cause);
    }

    public KubeApiException(Throwable cause) {
        super(cause);
    }
}
