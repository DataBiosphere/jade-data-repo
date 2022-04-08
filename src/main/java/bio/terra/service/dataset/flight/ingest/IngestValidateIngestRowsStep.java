package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidIngestDuplicatesException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestValidateIngestRowsStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(IngestValidateIngestRowsStep.class);
  private final DatasetService datasetService;

  public IngestValidateIngestRowsStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);

    if (targetTable.getPrimaryKey() != null && !targetTable.getPrimaryKey().isEmpty()) {
      if (BigQueryPdao.hasDuplicatePrimaryKeys(
          dataset, targetTable.getPrimaryKey(), stagingTableName)) {
        throw new InvalidIngestDuplicatesException(
            "Duplicate primary key values identified.",
            List.of(
                "Please ensure that you are not ingesting several rows with the same "
                    + "primary key values"));
      }
    }

    // TODO<DR-2407>: add something to ingest statistics
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
