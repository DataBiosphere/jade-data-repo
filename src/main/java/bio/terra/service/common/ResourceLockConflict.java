package bio.terra.service.common;

import bio.terra.common.exception.ConflictException;
import java.util.List;

public class ResourceLockConflict extends ConflictException {
  public ResourceLockConflict(String message) {
    super(message);
  }

  public ResourceLockConflict(String message, List<String> causes) {
    super(message, causes);
  }
}
