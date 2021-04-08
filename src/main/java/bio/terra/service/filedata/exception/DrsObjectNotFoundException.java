package bio.terra.service.filedata.exception;

import bio.terra.common.exception.NotFoundException;

/*
XXX: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and any
consequences downstream to DRS clients.
*/
public class DrsObjectNotFoundException extends NotFoundException {
    public DrsObjectNotFoundException(String message) {
        super(message);
    }

    public DrsObjectNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DrsObjectNotFoundException(Throwable cause) {
        super(cause);
    }
}
