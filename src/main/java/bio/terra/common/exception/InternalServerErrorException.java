package bio.terra.common.exception;

import java.util.List;

public class InternalServerErrorException extends DataRepoException {
    public InternalServerErrorException(String message) {
        super(message);
    }

    public InternalServerErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public InternalServerErrorException(Throwable cause) {
        super(cause);
    }

    public InternalServerErrorException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public InternalServerErrorException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }

}
