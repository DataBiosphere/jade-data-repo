package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public record CreateSnapshotIdStep(SnapshotRequestModel snapshotReq) implements Step {
  @Override
  public StepResult doStep(FlightContext context) {
    try {
      FlightMap workingMap = context.getWorkingMap();
      UUID snapshotId = UUID.randomUUID();
      workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, snapshotId);
      return StepResult.getStepResultSuccess();
    } catch (Exception ex) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new InvalidSnapshotException("Cannot create snapshot: " + snapshotReq.getName(), ex));
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
