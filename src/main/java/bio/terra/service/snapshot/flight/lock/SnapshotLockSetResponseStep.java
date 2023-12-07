package bio.terra.service.snapshot.flight.lock;

import bio.terra.model.ResourceLocks;
import bio.terra.service.common.ResourceLockSetResponseStep;
import bio.terra.service.snapshot.SnapshotService;
import java.util.UUID;

public class SnapshotLockSetResponseStep extends ResourceLockSetResponseStep {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotLockSetResponseStep(SnapshotService snapshotService, UUID snapshotId) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  protected ResourceLocks getResourceLocks() {
    return snapshotService.retrieveSnapshotSummary(snapshotId).getResourceLocks();
  }
}
