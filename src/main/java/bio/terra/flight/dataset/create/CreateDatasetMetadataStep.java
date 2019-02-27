package bio.terra.flight.dataset.create;

import bio.terra.dao.DatasetDao;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class CreateDatasetMetadataStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger("bio.terra.flight.dataset");

    private DatasetDao datasetDao;
    private DatasetService datasetService;

    public CreateDatasetMetadataStep(DatasetDao datasetDao, DatasetService datasetService) {
        this.datasetDao = datasetDao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        DatasetRequestModel datasetRequest = inputParameters.get("request", DatasetRequestModel.class);
        Dataset dataset = datasetService.makeDatasetFromDatasetRequest(datasetRequest);
        datasetDao.create(dataset);
        DatasetSummary datasetSummary = datasetDao.retrieveDatasetSummary(dataset.getId());
        DatasetSummaryModel response = datasetService.makeSummaryModelFromSummary(datasetSummary);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), response);
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DatasetRequestModel datasetRequest = inputParameters.get("request", DatasetRequestModel.class);
        String datasetName = datasetRequest.getName();
        datasetDao.deleteByName(datasetName);
        return StepResult.getStepResultSuccess();
    }
}

