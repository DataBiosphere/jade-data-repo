package bio.terra.service.snapshot.flight.export;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class SnapshotExportCreateBucketStep extends DefaultUndoStep {

  private final ResourceService resourceService;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotExportCreateBucketStep(
      ResourceService resourceService, SnapshotService snapshotService, UUID snapshotId) {
    this.resourceService = resourceService;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    try {
      GoogleBucketResource bucketForSnapshot =
          resourceService.getOrCreateBucketForSnapshotExport(snapshot, context.getFlightId());
      workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, bucketForSnapshot);
    } catch (BucketLockException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    } catch (GoogleResourceNamingException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
    return StepResult.getStepResultSuccess();
  }
}
