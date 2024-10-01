package bio.terra.service.filedata.exception;

import bio.terra.common.exception.NotFoundException;
import java.util.List;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
public class DrsObjectNotFoundException extends NotFoundException {
  public DrsObjectNotFoundException(String message) {
    super(message);
  }

  public DrsObjectNotFoundException(String message, List<String> causes) {
    super(message, causes);
  }
}
