package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class MaxDrsLookupsException extends BadRequestException {
    public MaxDrsLookupsException(String message) {
        super(message);
    }

    public MaxDrsLookupsException(String message, Throwable cause) {
        super(message, cause);
    }

    public MaxDrsLookupsException(Throwable cause) {
        super(cause);
    }
}
