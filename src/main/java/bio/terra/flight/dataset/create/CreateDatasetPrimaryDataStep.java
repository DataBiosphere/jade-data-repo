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

    Dataset getDataset(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DatasetRequestModel datasetRequest = inputParameters.get("request", DatasetRequestModel.class);
        return datasetService.makeDatasetFromDatasetRequest(datasetRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        bigQueryPdao.createDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }
}

