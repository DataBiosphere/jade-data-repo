package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.model.IngestRequestModel;
import bio.terra.common.PdaoLoadStatistics;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestLoadTableStep implements Step {
    private DatasetService datasetService;
    private BigQueryPdao bigQueryPdao;

    public IngestLoadTableStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
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
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.deleteDatasetTable(dataset, stagingTableName);
        return StepResult.getStepResultSuccess();
    }
}
