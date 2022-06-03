package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidIngestRowResolutionException;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestValidateTargetRowsStep implements Step {
  private static final int MAX_ERROR_MISMATCHED_ROWS = 20;
  private static final Logger logger = LoggerFactory.getLogger(IngestValidateTargetRowsStep.class);
  private final DatasetService datasetService;

  public IngestValidateTargetRowsStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    UUID transactionId = TransactionUtils.getTransactionId(context);

    TableResult mismatchedMergeRows =
        BigQueryDatasetPdao.stagingRowsWithoutSingleTargetRowMatch(
            dataset, targetTable, stagingTableName, transactionId);
    long numMismatchedMergeRows = mismatchedMergeRows.getTotalRows();

    if (numMismatchedMergeRows > 0) {
      StringBuffer errorMessage =
          new StringBuffer("Some merge record(s) did not resolve to a single target record (");

      List<String> errorDetails = new ArrayList<>();
      int count = 0;
      for (FieldValueList row : mismatchedMergeRows.iterateAll()) {
        if (count >= MAX_ERROR_MISMATCHED_ROWS) {
          errorMessage.append(MAX_ERROR_MISMATCHED_ROWS + " out of ");
          break;
        }
        errorDetails.add(
            row.get("numTargetRows").getStringValue()
                + " rows in target table with "
                + targetTable.primaryKeyToString(row));
        count++;
      }

      errorMessage.append(numMismatchedMergeRows + " returned in details)");

      throw new InvalidIngestRowResolutionException(errorMessage.toString(), errorDetails);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
