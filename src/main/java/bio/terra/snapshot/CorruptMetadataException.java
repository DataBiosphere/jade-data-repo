package bio.terra.snapshot;

import bio.terra.common.exception.InternalServerErrorException;

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
