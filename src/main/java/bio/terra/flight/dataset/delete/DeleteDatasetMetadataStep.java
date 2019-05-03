package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FileDao;
import bio.terra.flight.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDatasetMetadataStep implements Step {

    private DatasetDao datasetDao;
    private UUID datasetId;
    private FileDao fileDao;

    public DeleteDatasetMetadataStep(DatasetDao datasetDao, UUID datasetId, FileDao fileDao) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        fileDao.deleteDatasetFileDependencies(datasetId);
        boolean found = datasetDao.delete(datasetId);
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

