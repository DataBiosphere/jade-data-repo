package bio.terra.service.snapshot.flight.unlock;

import bio.terra.service.common.UnlockResourceCheckLockNameStep;
import bio.terra.service.snapshot.SnapshotService;
import java.util.UUID;

public class UnlockSnapshotCheckLockNameStep extends UnlockResourceCheckLockNameStep {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public UnlockSnapshotCheckLockNameStep(
      SnapshotService snapshotService, UUID snapshotId, String lockName) {
    super(lockName);
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  protected String getExclusiveLock() {
    return snapshotService.retrieveSnapshotSummary(snapshotId).getResourceLocks().getExclusive();
  }
}
