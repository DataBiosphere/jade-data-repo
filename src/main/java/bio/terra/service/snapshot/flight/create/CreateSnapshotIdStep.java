package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CreateSnapshotIdStep implements Step {
    private final SnapshotRequestModel snapshotReq;

    private static Logger logger = LoggerFactory.getLogger(CreateSnapshotIdStep.class);

    public CreateSnapshotIdStep(
        SnapshotRequestModel snapshotReq) {
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            FlightMap workingMap = context.getWorkingMap();
            UUID snapshotId = UUID.randomUUID();
            workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, snapshotId);
            return StepResult.getStepResultSuccess();
        } catch (InvalidSnapshotException isEx) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, isEx);
        } catch (Exception ex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new InvalidSnapshotException("Cannot create dataset: " + snapshotReq.getName(), ex));
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        logger.debug("Snapshot creation failed during id creation.");
        return StepResult.getStepResultSuccess();
    }

}
