package bio.terra.service.snapshot.flight.export;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class SnapshotExportValidateExportParametersStep implements Step {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotExportValidateExportParametersStep(
      SnapshotService snapshotService, UUID snapshotId) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap inputParameters = context.getInputParameters();
    Boolean exportGsPaths = inputParameters.get(ExportMapKeys.EXPORT_GSPATHS, Boolean.class);
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    if (snapshot.isSelfHosted() && exportGsPaths) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new UnsupportedOperationException("Cannot export GS Paths for self-hosted snapshots"));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
