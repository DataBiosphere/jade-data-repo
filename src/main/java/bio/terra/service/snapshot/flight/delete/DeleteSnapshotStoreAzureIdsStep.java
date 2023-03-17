package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class DeleteSnapshotStoreAzureIdsStep implements Step {
  private final UUID snapshotId;
  private final SnapshotService snapshotService;

  public DeleteSnapshotStoreAzureIdsStep(UUID snapshotId, SnapshotService snapshotService) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    UUID profileId = snapshot.getProfileId();
    UUID storageResourceId = snapshot.getStorageAccountResource().getResourceId();

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(SnapshotWorkingMapKeys.PROFILE_ID, profileId);
    workingMap.put(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID, storageResourceId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
