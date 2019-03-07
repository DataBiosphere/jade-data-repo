package bio.terra.flight.dataset.create;

import bio.terra.dao.DatasetDao;
import bio.terra.exceptions.ValidationException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.RowIdMatch;
import bio.terra.model.DatasetRequestContentsModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

public class CreateDatasetPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;
    private DatasetDao datasetDao;

    public CreateDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        DatasetService datasetService,
                                        DatasetDao datasetDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.datasetDao = datasetDao;
    }

    DatasetRequestModel getRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get("request", DatasetRequestModel.class);
    }

    Dataset getDataset(FlightContext context) {
        DatasetRequestModel datasetRequest = getRequestModel(context);
        return datasetService.makeDatasetFromDatasetRequest(datasetRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
         * map field ids into row ids and validate
         * then pass the row id array into create dataset
         */
        DatasetRequestModel requestModel = getRequestModel(context);
        DatasetRequestContentsModel contentsModel = requestModel.getContents().get(0);

        Dataset dataset = datasetDao.retrieveDatasetByName(requestModel.getName());
        DatasetSource source = dataset.getDatasetSources().get(0);
        RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(dataset, source, contentsModel.getRootValues());
        if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
            String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
            String message = String.format("Mismatched input values: '%s'", unmatchedValues);
            ErrorModel errorModel = new ErrorModel().message(message);
            setResponse(context, errorModel, HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new ValidationException(message));
        }

        bigQueryPdao.createDataset(dataset, rowIdMatch.getMatchingRowIds());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }

    // TODO: this is a copy of what is in the metadata step. Should make a common utility module for
    // things like this.
    private void setResponse(FlightContext context, Object responseObject, HttpStatus responseStatus) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), responseObject);
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), responseStatus);
    }

}

