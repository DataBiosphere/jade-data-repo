package bio.terra.flight.snapshot.delete;

import bio.terra.snapshot.SnapshotDao;
import bio.terra.dao.exception.SnapshotNotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Snapshot;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteSnapshotMetadataStep implements Step {

    private SnapshotDao snapshotDao;
    private UUID snapshotId;

    public DeleteSnapshotMetadataStep(SnapshotDao snapshotDao, UUID snapshotId) {
        this.snapshotDao = snapshotDao;
        this.snapshotId = snapshotId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Snapshot snapshot = null;
        boolean found = false;
        try {

            found = snapshotDao.delete(snapshotId);
        } catch (SnapshotNotFoundException ex) {
            found = false;
        }

        DeleteResponseModel.ObjectStateEnum stateEnum =
            (found) ? DeleteResponseModel.ObjectStateEnum.DELETED : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
        DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
        FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // This step is not undoable. We only get here when the
        // do method has a dismal failure.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalStateException("Attempt to undo permanent delete"));
    }
}

