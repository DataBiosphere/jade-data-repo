package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoConstant;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidIngestDuplicatesException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestValidateIngestRowsStep implements Step {
  private static final int MAX_ERROR_DUPLICATE_ROWS = 20;
  private static final Logger logger = LoggerFactory.getLogger(IngestValidateIngestRowsStep.class);
  private final DatasetService datasetService;
  private final boolean postIngest;

  public IngestValidateIngestRowsStep(DatasetService datasetService, boolean postIngest) {
    this.datasetService = datasetService;
    this.postIngest = postIngest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);

    if (targetTable.getPrimaryKey() != null && !targetTable.getPrimaryKey().isEmpty()) {
      // targetTable.getPrimaryKey() would be the same as stagingTable.getPrimaryKey()
      // checks for duplicate primary keys in the specified table
      String tableNameToCheck =
          postIngest
              ? targetTable.getName()
              : IngestUtils.getStagingTableName(
                  context); // use target table for post-ingest validation
      TableResult duplicatePrimaryKeys =
          BigQueryPdao.duplicatePrimaryKeys(dataset, targetTable.getPrimaryKey(), tableNameToCheck);
      long numDuplicatePrimaryKeys = duplicatePrimaryKeys.getTotalRows();

      if (numDuplicatePrimaryKeys > 0) {
        StringBuffer errorMessage = new StringBuffer("Duplicate primary keys identified (");

        List<String> errorDetails = new ArrayList<>();
        int count = 0;
        for (FieldValueList row : duplicatePrimaryKeys.iterateAll()) {
          if (count >= MAX_ERROR_DUPLICATE_ROWS) {
            errorMessage.append(MAX_ERROR_DUPLICATE_ROWS + " out of ");
            break;
          }
          errorDetails.add(
              row.get(PdaoConstant.PDAO_COUNT_ALIAS).getStringValue()
                  + " ingest rows with "
                  + targetTable.primaryKeyToString(row));
          count++;
        }

        errorMessage.append(numDuplicatePrimaryKeys + " returned in details)");

        throw new InvalidIngestDuplicatesException(errorMessage.toString(), errorDetails);
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
