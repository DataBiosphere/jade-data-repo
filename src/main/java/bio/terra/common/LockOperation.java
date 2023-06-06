package bio.terra.common;

import java.util.List;

/** Actions which could be taken to modify the locks held on a resource (dataset, snapshot). */
public enum LockOperation {
  LockExclusive,
  LockShared,
  UnlockExclusive,
  UnlockShared;

  /**
   * @return additional guidance for users investigating a failed lock operation, usually as part of
   *     a failed job / flight.
   */
  public List<String> getErrorDetails() {
    var template =
        "A failure to obtain %s on a resource likely means that it's already %s by another process.";
    return switch (this) {
      case LockShared -> List.of(template.formatted("a shared lock", "exclusively locked"));
      case LockExclusive -> List.of(template.formatted("an exclusive lock", "locked"));
      default -> List.of();
    };
  }

  /**
   * @return whether this lock operation represents an attempted lock application (rather than
   *     unlock)
   */
  public boolean lockAttempted() {
    return this.equals(LockExclusive) || this.equals(LockShared);
  }
}
