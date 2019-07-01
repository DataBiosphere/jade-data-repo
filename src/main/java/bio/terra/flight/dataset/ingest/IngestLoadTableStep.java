package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestLoadTableStep implements Step {
    private DrDatasetDao datasetDao;
    private BigQueryPdao bigQueryPdao;

    public IngestLoadTableStep(DrDatasetDao datasetDao, BigQueryPdao bigQueryPdao) {
        this.datasetDao = datasetDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = IngestUtils.getDataset(context, datasetDao);
        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

        PdaoLoadStatistics ingestStatistics = bigQueryPdao.loadToStagingTable(
            dataset,
            targetTable,
            stagingTableName,
            ingestRequest);

        // Save away the stats in the working map. We will use some of them later
        // when we make the annotations. Others are returned on the ingest response.
        IngestUtils.putIngestStatistics(context, ingestStatistics);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        DrDataset dataset = IngestUtils.getDataset(context, datasetDao);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.deleteTable(dataset.getName(), stagingTableName);
        return StepResult.getStepResultSuccess();
    }
}
