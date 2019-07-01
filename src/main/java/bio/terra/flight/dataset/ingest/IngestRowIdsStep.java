package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.metadata.DrDataset;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestRowIdsStep implements Step {
    private DrDatasetDao datasetDao;
    private BigQueryPdao bigQueryPdao;

    public IngestRowIdsStep(DrDatasetDao datasetDao, BigQueryPdao bigQueryPdao) {
        this.datasetDao = datasetDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = IngestUtils.getDataset(context, datasetDao);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.addRowIdsToStagingTable(dataset, stagingTableName);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be restarted on failure.
        return StepResult.getStepResultSuccess();
    }
}
