package bio.terra.flight.dataset.create;

import bio.terra.metadata.DrDataset;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

public class CreateDrDatasetPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;

    public CreateDrDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    DrDataset getDataset(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DrDatasetRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
            DrDatasetRequestModel.class);
        return DrDatasetJsonConversion.datasetRequestToDataset(datasetRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = getDataset(context);
        bigQueryPdao.createDataset(dataset);
        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }
}

