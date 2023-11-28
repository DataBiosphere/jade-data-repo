package bio.terra.service.snapshot.flight.lock;

import bio.terra.common.FlightUtils;
import bio.terra.model.ResourceLocks;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class SnapshotLockSetResponseStep extends DefaultUndoStep {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotLockSetResponseStep(SnapshotService snapshotService, UUID snapshotId) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    var snapshotSummaryModel = snapshotService.retrieveSnapshotSummary(snapshotId);
    ResourceLocks locks = snapshotSummaryModel.getResourceLocks();
    FlightUtils.setResponse(context, locks, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }
}
