package bio.terra.common;

import bio.terra.model.ResourceLocks;
import java.util.ArrayList;
import java.util.List;

/** Actions which could be taken to modify the locks held on a resource (dataset, snapshot). */
public enum LockOperation {
  LOCK_EXCLUSIVE(true, "an exclusive lock", "locked"),
  LOCK_SHARED(true, "a shared lock", "exclusively locked"),
  UNLOCK_EXCLUSIVE(false),
  UNLOCK_SHARED(false);

  private final boolean isLock;
  private final String description;
  private final String conflict;

  LockOperation(boolean isLock, String description, String conflict) {
    this.isLock = isLock;
    this.description = description;
    this.conflict = conflict;
  }

  LockOperation(boolean isLock) {
    this(isLock, null, null);
  }

  /**
   * @return additional guidance for users investigating a failed lock operation, usually as part of
   *     a failed job / flight.
   */
  public List<String> getErrorDetails(ResourceLocks conflictingLocks) {
    var template =
        "A failure to obtain %s on a resource likely means that it's already %s by another process.";
    if (description != null && conflict != null) {
      if (conflictingLocks != null && !isEmpty(conflictingLocks)) {
        template =
            template
                + " Conflicting lock(s): %s. You can remove a lock by using the unlock API endpoint with the ID of the resource (Snapshot or Dataset), the name of the lock, and forceUnlock set to false.";
        if (this == LOCK_EXCLUSIVE) {
          // include any locks that exist both exclusive and shared (but only one will be populated)
          List<String> lockList = new ArrayList<>();
          if (isExclusive(conflictingLocks)) lockList.add(conflictingLocks.getExclusive());
          if (areShared(conflictingLocks)) lockList.addAll(conflictingLocks.getShared());
          String locks = String.join(", ", lockList);
          return List.of(template.formatted(description, conflict, locks));
        } else if (this == LOCK_SHARED) {
          // include only exclusive locks if the lock operation attempted was shared
          return List.of(
              template.formatted(description, conflict, conflictingLocks.getExclusive()));
        }
      }
      return List.of(template.formatted(description, conflict));
    }
    return List.of();
  }

  public List<String> getErrorDetails() {
    return getErrorDetails(null);
  }

  /**
   * @return whether this lock operation represents an attempted lock application (rather than
   *     unlock)
   */
  public boolean lockAttempted() {
    return isLock;
  }

  private boolean isEmpty(ResourceLocks locks) {
    return !isExclusive(locks) && !areShared(locks);
  }

  private boolean isExclusive(ResourceLocks locks) {
    return locks.getExclusive() != null && !locks.getExclusive().isEmpty();
  }

  private boolean areShared(ResourceLocks locks) {
    return locks.getShared() != null && !locks.getShared().isEmpty();
  }
}
