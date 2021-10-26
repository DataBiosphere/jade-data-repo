package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getSuffix;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateExternalTablesStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final DatasetService datasetService;

  private static Logger logger = LoggerFactory.getLogger(CreateExternalTablesStep.class);

  public CreateExternalTablesStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
    this.bigQueryPdao = bigQueryPdao;
    this.datasetService = datasetService;
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
    String suffix = getSuffix(context);
    List<DataDeletionTableModel> tables =
        FlightUtils.getTyped(context.getWorkingMap(), DataDeletionMapKeys.TABLES);

    validateTablesExistInDataset(tables, dataset);

    for (DataDeletionTableModel table : tables) {
      String path = table.getGcsFileSpec().getPath();
      // let any exception here trigger an undo, no use trying to continue
      bigQueryPdao.createSoftDeleteExternalTable(dataset, path, table.getTableName(), suffix);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    Dataset dataset = getDataset(context, datasetService);
    String suffix = getSuffix(context);

    for (DataDeletionTableModel table : getRequest(context).getTables()) {
      try {
        bigQueryPdao.deleteSoftDeleteExternalTable(dataset, table.getTableName(), suffix);
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
