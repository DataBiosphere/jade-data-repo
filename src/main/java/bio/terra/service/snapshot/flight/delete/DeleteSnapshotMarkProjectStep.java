package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.resourcemanagement.ResourceService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotMarkProjectStep implements Step {

  private final ResourceService resourceService;
  private final UUID snapshotId;
  private final SnapshotService snapshotService;

  public DeleteSnapshotMarkProjectStep(ResourceService resourceService, UUID snapshotId, SnapshotService snapshotService) {
    this.resourceService = resourceService;
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
  }

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotMarkProjectStep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    List<UUID> projectResourceIds = List.of(snapshot.getProjectResourceId());
    resourceService.markProjectsForDelete(projectResourceIds);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_PROJECT_ID_LIST, projectResourceIds);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
