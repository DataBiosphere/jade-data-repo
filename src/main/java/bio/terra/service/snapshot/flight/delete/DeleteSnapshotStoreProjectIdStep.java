package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class DeleteSnapshotStoreProjectIdStep implements Step {
  private final UUID snapshotId;
  private final SnapshotService snapshotService;

  public DeleteSnapshotStoreProjectIdStep(UUID snapshotId, SnapshotService snapshotService) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    List<UUID> projectResourceIds = List.of(snapshot.getProjectResourceId());

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_PROJECT_ID_LIST, projectResourceIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
