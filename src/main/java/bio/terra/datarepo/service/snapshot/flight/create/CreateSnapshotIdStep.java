package bio.terra.datarepo.service.snapshot.flight.create;

import bio.terra.datarepo.model.SnapshotRequestModel;
import bio.terra.datarepo.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.datarepo.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSnapshotIdStep implements Step {
  private final SnapshotRequestModel snapshotReq;

  private static Logger logger = LoggerFactory.getLogger(CreateSnapshotIdStep.class);

  public CreateSnapshotIdStep(SnapshotRequestModel snapshotReq) {
    this.snapshotReq = snapshotReq;
  }

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
