package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Dataset;
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

    public DeleteDatasetMetadataStep(DatasetDao datasetDao, UUID datasetId) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = null;
        boolean found = false;
        try {

            found = datasetDao.delete(datasetId);
        } catch (DatasetNotFoundException ex) {
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

