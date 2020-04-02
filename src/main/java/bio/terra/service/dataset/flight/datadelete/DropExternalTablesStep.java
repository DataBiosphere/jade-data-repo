package bio.terra.service.dataset.flight.datadelete;

import bio.terra.model.DataDeletionRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class DropExternalTablesStep implements Step {

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;

    private static Logger logger = LoggerFactory.getLogger(DropExternalTablesStep.class);

    public DropExternalTablesStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    private String suffix(FlightContext context) {
        return context.getFlightId().replace('-', '_');
    }

    private DataDeletionRequest request(FlightContext context) {
        return context.getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
    }

    private Dataset dataset(FlightContext context) {
        String datasetId = context.getInputParameters()
            .get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        return datasetService.retrieve(UUID.fromString(datasetId));
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = dataset(context);
        String suffix = suffix(context);
        DataDeletionRequest dataDeletionRequest = request(context);

        dataDeletionRequest.getTables().forEach(table ->
            bigQueryPdao.deleteExternalTable(dataset, table.getTableName(), suffix));

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO: need a human to intervene -- the soft delete worked but we couldn't clean up tables
        return StepResult.getStepResultSuccess();
    }
}

