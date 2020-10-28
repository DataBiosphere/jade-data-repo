package bio.terra.service.dataset.flight.datadelete;

import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getSuffix;

public class DropExternalTablesStep implements Step {

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;

    private static Logger logger = LoggerFactory.getLogger(DropExternalTablesStep.class);

    public DropExternalTablesStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        Dataset dataset = getDataset(context, datasetService);
        String suffix = getSuffix(context);
        DataDeletionRequest dataDeletionRequest = getRequest(context);

        for (DataDeletionTableModel table : dataDeletionRequest.getTables()) {
            bigQueryPdao.deleteSoftDeleteExternalTable(dataset, table.getTableName(), suffix);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO: need a human to intervene -- the soft delete worked but we couldn't clean up tables
        return StepResult.getStepResultSuccess();
    }
}

