package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateAddTablesBigQueryStep implements Step {
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetDao datasetDao;
  private final UUID datasetId;

  private final DatasetSchemaUpdateModel updateModel;

  public DatasetSchemaUpdateAddTablesBigQueryStep(
      BigQueryDatasetPdao bigQueryDatasetPdao,
      DatasetDao datasetDao,
      UUID datasetId,
      DatasetSchemaUpdateModel updateModel) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetDao.retrieve(datasetId);
    List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
    List<DatasetTable> tables =
        dataset.getTables().stream()
            .filter(dt -> newTableNames.contains(dt.getName()))
            .collect(Collectors.toList());
    ;
    bigQueryDatasetPdao.createTables(dataset, tables);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetDao.retrieve(datasetId);
    List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
    for (String tableName : newTableNames) {
      bigQueryDatasetPdao.deleteDatasetTable(dataset, tableName);
    }
    return StepResult.getStepResultSuccess();
  }
}
