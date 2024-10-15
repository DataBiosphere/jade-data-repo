package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.ResourceLocks;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class LockOperationTest {
  ResourceLocks conflictingSharedLocks;
  ResourceLocks conflictingExclusiveLocks;

  @BeforeEach
  public void setup() {
    conflictingSharedLocks = new ResourceLocks().addSharedItem("shared1").addSharedItem("shared2");
    conflictingExclusiveLocks = new ResourceLocks().exclusive("exclusive");
  }

  @Test
  void getErrorDetailsUnlockShared() {
    assertThat(LockOperation.UNLOCK_SHARED.getErrorDetails(), equalTo(List.of()));
  }

  @Test
  void getErrorDetailsUnlockExclusive() {
    assertThat(LockOperation.UNLOCK_EXCLUSIVE.getErrorDetails(), equalTo(List.of()));
  }

  @Test
  void getErrorDetailsUnlockSharedWithLocks() {
    assertThat(
        LockOperation.UNLOCK_SHARED.getErrorDetails(conflictingExclusiveLocks), equalTo(List.of()));
  }

  @Test
  void getErrorDetailsUnlockExclusiveWithLocks() {
    assertThat(
        LockOperation.UNLOCK_EXCLUSIVE.getErrorDetails(conflictingExclusiveLocks),
        equalTo(List.of()));
  }

  @Test
  void getErrorDetailsExclusiveLockNoConflictingLocks() {
    assertThat(
        LockOperation.LOCK_EXCLUSIVE.getErrorDetails(),
        equalTo(
            List.of(
                "A failure to obtain an exclusive lock on a resource likely means that it's already locked by another process.")));
  }

  @Test
  void getErrorDetailsSharedLockNoConflictingLocks() {
    assertThat(
        LockOperation.LOCK_SHARED.getErrorDetails(),
        equalTo(
            List.of(
                "A failure to obtain a shared lock on a resource likely means that it's already exclusively locked by another process.")));
  }

  @Test
  void getErrorDetailsExclusiveLockConflictingLocks() {
    assertThat(
        LockOperation.LOCK_EXCLUSIVE.getErrorDetails(conflictingExclusiveLocks),
        equalTo(
            List.of(
                "A failure to obtain an exclusive lock on a resource likely means that it's already locked by another process. Conflicting lock(s): exclusive. You can remove a lock by using the unlock API endpoint with the ID of the resource (Snapshot or Dataset), the name of the lock, and forceUnlock set to false.")));
    assertThat(
        LockOperation.LOCK_EXCLUSIVE.getErrorDetails(conflictingSharedLocks),
        equalTo(
            List.of(
                "A failure to obtain an exclusive lock on a resource likely means that it's already locked by another process. Conflicting lock(s): shared1, shared2. You can remove a lock by using the unlock API endpoint with the ID of the resource (Snapshot or Dataset), the name of the lock, and forceUnlock set to false.")));
  }

  @Test
  void getErrorDetailsSharedLockConflictingLocks() {
    assertThat(
        LockOperation.LOCK_SHARED.getErrorDetails(conflictingExclusiveLocks),
        equalTo(
            List.of(
                "A failure to obtain a shared lock on a resource likely means that it's already exclusively locked by another process. Conflicting lock(s): exclusive. You can remove a lock by using the unlock API endpoint with the ID of the resource (Snapshot or Dataset), the name of the lock, and forceUnlock set to false.")));
  }
}
