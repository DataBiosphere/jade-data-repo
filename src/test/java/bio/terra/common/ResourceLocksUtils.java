package bio.terra.common;

import bio.terra.model.ResourceLocks;
import java.util.List;
import java.util.Optional;

public class ResourceLocksUtils {

  /**
   * @return the lock object's exclusive lock, or null if not present
   */
  public static String getExclusiveLock(ResourceLocks resourceLocks) {
    return Optional.ofNullable(resourceLocks).map(ResourceLocks::getExclusive).orElse(null);
  }

  public static List<String> getSharedLock(ResourceLocks resourceLocks) {
    return Optional.ofNullable(resourceLocks).map(ResourceLocks::getShared).orElse(null);
  }
}
