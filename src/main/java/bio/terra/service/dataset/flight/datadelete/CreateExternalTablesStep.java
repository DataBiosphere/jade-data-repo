package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateExternalTablesStep implements Step {

  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetService datasetService;
  private final GcsConfiguration gcsConfiguration;

  private static Logger logger = LoggerFactory.getLogger(CreateExternalTablesStep.class);

  public CreateExternalTablesStep(
      BigQueryDatasetPdao bigQueryDatasetPdao,
      DatasetService datasetService,
      GcsConfiguration gcsConfiguration) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetService = datasetService;
    this.gcsConfiguration = gcsConfiguration;
  }

  private void validateTablesExistInDataset(List<DataDeletionTableModel> tables, Dataset dataset) {
    List<String> missingTables =
        tables.stream()
            .filter(t -> !dataset.getTableByName(t.getTableName()).isPresent())
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());

    if (missingTables.size() > 0) {
      throw new TableNotFoundException(
          "Not all tables from request exist in dataset: " + String.join(", ", missingTables));
    }
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context, datasetService);
    String suffix = BigQueryUtils.getSuffix(context);
    List<DataDeletionTableModel> tables =
        FlightUtils.getTyped(context.getWorkingMap(), DataDeletionMapKeys.TABLES);

    validateTablesExistInDataset(tables, dataset);

    // At this point, all table models have a GcsFileSpec
    for (DataDeletionTableModel table : tables) {
      String path = table.getGcsFileSpec().getPath();
      // let any exception here trigger an undo, no use trying to continue
      bigQueryDatasetPdao.createSoftDeleteExternalTable(
          dataset, path, table.getTableName(), suffix);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    Dataset dataset = getDataset(context, datasetService);
    String suffix = BigQueryUtils.getSuffix(context);

    for (DataDeletionTableModel table : getRequest(context).getTables()) {
      try {
        BigQueryPdao.deleteExternalTable(
            dataset,
            table.getTableName(),
            suffix,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());
      } catch (Exception ex) {
        // catch any exception and get it into the log, make a
        String msg =
            String.format(
                "Couldn't clean up external table for %s from dataset %s w/ suffix %s",
                table.getTableName(), dataset.getName(), suffix);
        logger.warn(msg, ex);
      }
    }

    return StepResult.getStepResultSuccess();
  }
}
