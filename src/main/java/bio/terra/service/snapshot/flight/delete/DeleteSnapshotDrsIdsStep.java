package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.filedata.DrsService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotDrsIdsStep implements Step {

  private static final Logger logger = LoggerFactory.getLogger(DeleteSnapshotDrsIdsStep.class);

  private final DrsService drsService;
  private final UUID snapshotId;

  public DeleteSnapshotDrsIdsStep(DrsService drsService, UUID snapshotId) {
    this.drsService = drsService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    logger.info("Deleted {} rows", drsService.deleteDrsIdToSnapshotsBySnapshot(snapshotId));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // This step is not undoable. We only get here when the
    // do method has a dismal failure.
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
