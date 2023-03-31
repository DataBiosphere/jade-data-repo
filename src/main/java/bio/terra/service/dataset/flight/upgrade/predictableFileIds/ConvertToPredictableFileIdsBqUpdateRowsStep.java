package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.common.Column;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsBqUpdateRowsStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqUpdateRowsStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final AuthenticatedUserRequest authedUser;

  public ConvertToPredictableFileIdsBqUpdateRowsStep(
      UUID datasetId,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AuthenticatedUserRequest authedUser) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.authedUser = authedUser;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    UUID transactionId = TransactionUtils.getTransactionId(context);
    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());

    if (oldToNewMappings.isEmpty()) {
      logger.info("No file ids to migrate");
      return StepResult.getStepResultSuccess();
    }
    // Load file ids into the table
    for (DatasetTable table : dataset.getTables()) {
      logger.info("Migrating table {}", table.getName());

      if (table.getColumns().stream().anyMatch(Column::isFileOrDirRef)) {
        bigQueryDatasetPdao.insertNewFileIdsIntoDatasetTable(
            dataset, table, transactionId, context.getFlightId(), authedUser);
      }
    }

    return StepResult.getStepResultSuccess();
  }
}
