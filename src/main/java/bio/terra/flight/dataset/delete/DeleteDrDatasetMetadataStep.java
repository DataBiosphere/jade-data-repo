package bio.terra.flight.dataset.delete;

import bio.terra.dao.DrDatasetDao;
import bio.terra.flight.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDrDatasetMetadataStep implements Step {
    private DrDatasetDao datasetDao;

    public DeleteDrDatasetMetadataStep(DrDatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID datasetId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        boolean success = datasetDao.delete(datasetId);
        DeleteResponseModel.ObjectStateEnum stateEnum =
            (success) ? DeleteResponseModel.ObjectStateEnum.DELETED : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
        DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
        FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // no undo is possible
        return StepResult.getStepResultSuccess();
    }

}
