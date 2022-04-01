package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestLoadTableStep implements Step {
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public IngestLoadTableStep(
      DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao) {
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    FlightMap workingMap = context.getWorkingMap();

    String pathToIngestFile = workingMap.get(IngestMapKeys.INGEST_CONTROL_FILE_PATH, String.class);

    PdaoLoadStatistics ingestStatistics =
        bigQueryDatasetPdao.loadToStagingTable(
            dataset, targetTable, stagingTableName, ingestRequest, pathToIngestFile);

    // Save away the stats in the working map. We will use some of them later
    // when we make the annotations. Others are returned on the ingest response.
    IngestUtils.putIngestStatistics(context, ingestStatistics);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    bigQueryDatasetPdao.deleteDatasetTable(dataset, stagingTableName);
    return StepResult.getStepResultSuccess();
  }
}
