package bio.terra.flight.dataset.create;

import bio.terra.metadata.Dataset;
import bio.terra.model.DatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateDatasetPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;

    public CreateDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
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



        bigQueryPdao.createDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }
}

