package bio.terra.common;

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
  public List<String> getErrorDetails() {
    var template =
        "A failure to obtain %s on a resource likely means that it's already %s by another process.";
    if (description != null && conflict != null) {
      return List.of(template.formatted(description, conflict));
    }
    return List.of();
  }

  /**
   * @return whether this lock operation represents an attempted lock application (rather than
   *     unlock)
   */
  public boolean lockAttempted() {
    return isLock;
  }
}
