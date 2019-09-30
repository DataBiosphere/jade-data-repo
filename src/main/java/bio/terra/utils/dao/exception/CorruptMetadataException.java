package bio.terra.utils.dao.exception;

import bio.terra.exception.InternalServerErrorException;

public class CorruptMetadataException extends InternalServerErrorException {
    public CorruptMetadataException(String message) {
        super(message);
    }

    public CorruptMetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public CorruptMetadataException(Throwable cause) {
        super(cause);
    }
}
