package bio.terra.flight.datasnapshot.delete;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.dao.exception.DataSnapshotNotFoundException;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDataSnapshotMetadataStep implements Step {

    private DataSnapshotDao dataSnapshotDao;
    private UUID datasetId;
    private FireStoreDependencyDao dependencyDao;

    public DeleteDataSnapshotMetadataStep(
        DataSnapshotDao dataSnapshotDao, UUID datasetId, FireStoreDependencyDao dependencyDao) {
        this.dataSnapshotDao = dataSnapshotDao;
        this.datasetId = datasetId;
        this.dependencyDao = dependencyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DataSnapshot dataSnapshot = null;
        boolean found = false;
        try {
            dataSnapshot = dataSnapshotDao.retrieveDataSnapshot(datasetId);

            // Remove data snapshot file references from the underlying studies
            for (DataSnapshotSource dataSnapshotSource : dataSnapshot.getDataSnapshotSources()) {
                dependencyDao.deleteDataSnapshotFileDependencies(
                    dataSnapshotSource.getStudy().getId().toString(),
                    datasetId.toString());
            }
            found = dataSnapshotDao.delete(datasetId);
        } catch (DataSnapshotNotFoundException ex) {
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

