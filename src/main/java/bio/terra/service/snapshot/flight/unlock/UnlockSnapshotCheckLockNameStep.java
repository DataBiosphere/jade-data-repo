package bio.terra.service.snapshot.flight.unlock;

import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.UnlockResourceCheckLockNameStep;
import bio.terra.service.snapshot.SnapshotService;
import java.util.List;
import java.util.UUID;

public class UnlockSnapshotCheckLockNameStep extends UnlockResourceCheckLockNameStep {
  private final SnapshotService snapshotService;

  public UnlockSnapshotCheckLockNameStep(
      SnapshotService snapshotService, UUID snapshotId, String lockName) {
    super(IamResourceType.DATASNAPSHOT, snapshotId, lockName);
    this.snapshotService = snapshotService;
  }

  protected List<String> getLocks() {
    // Snapshots do not have shared locks, so we just return a list of one exclusive lock
    var exclusiveLock =
        snapshotService.retrieveSnapshotSummary(resourceId).getResourceLocks().getExclusive();
    if (exclusiveLock != null) {
      return List.of(exclusiveLock);
    } else {
      return List.of();
    }
  }

  // Snapshots do not have shared locks, so we always return false
  protected boolean isSharedLock(String lockName) {
    return false;
  }
}
