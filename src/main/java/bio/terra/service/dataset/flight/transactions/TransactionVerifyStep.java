package bio.terra.service.dataset.flight.transactions;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.TransactionCommitException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionVerifyStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionVerifyStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final UUID transactionId;

  public TransactionVerifyStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      UUID transactionId) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.transactionId = transactionId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    List<String> tablesWithConflicts = new ArrayList<>();
    for (DatasetTable datasetTable : dataset.getTables()) {
      if (datasetTable.getPrimaryKey() != null && !datasetTable.getPrimaryKey().isEmpty()) {
        long conflicts =
            bigQueryTransactionPdao.verifyTransaction(dataset, datasetTable, transactionId);
        if (conflicts > 0) {
          tablesWithConflicts.add(datasetTable.getName());
        }
      }
    }
    if (!tablesWithConflicts.isEmpty()) {
      throw new TransactionCommitException(
          "Could not commit the transaction because of conflicts",
          List.of(
              String.format(
                  "The following table(s) have conflicts: %s",
                  StringUtils.join(tablesWithConflicts, ","))));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
