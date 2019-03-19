package bio.terra.flight.dataset.create;

import bio.terra.dao.DatasetDao;
import bio.terra.exception.NotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSummary;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateDatasetMetadataStep implements Step {
    private DatasetDao datasetDao;
    private DatasetService datasetService;

    public CreateDatasetMetadataStep(DatasetDao datasetDao, DatasetService datasetService) {
        this.datasetDao = datasetDao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DatasetRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                DatasetRequestModel.class);
        try {
            Dataset dataset = datasetService.makeDatasetFromDatasetRequest(datasetRequest);
            UUID datasetId = datasetDao.create(dataset);
            context.getWorkingMap().put("datasetId", datasetId);
            DatasetSummary datasetSummary = datasetDao.retrieveDatasetSummary(dataset.getId());
            DatasetSummaryModel response = datasetService.makeSummaryModelFromSummary(datasetSummary);
            FlightUtils.setResponse(context, response, HttpStatus.CREATED);
            return StepResult.getStepResultSuccess();
        } catch (NotFoundException ex) {
            FlightUtils.setErrorResponse(context, ex.toString(), HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DatasetRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                DatasetRequestModel.class);
        String datasetName = datasetRequest.getName();
        datasetDao.deleteByName(datasetName);
        return StepResult.getStepResultSuccess();
    }

}
