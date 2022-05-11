package bio.terra.service.dataset.flight.update;

import bio.terra.common.Column;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.bigquery.BigQuery;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DatasetSchemaUpdateAddColumnsBigQueryStep implements Step {
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetDao datasetDao;
  private final UUID datasetId;
  private final DatasetSchemaUpdateModel updateModel;

  private static final List<String> EMPTY_LIST = Collections.emptyList();

  public DatasetSchemaUpdateAddColumnsBigQueryStep(
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

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    for (var columnChanges : updateModel.getChanges().getAddColumns()) {
      DatasetTable table = dataset.getTableByName(columnChanges.getTableName()).orElseThrow();
      for (ColumnModel columnModel : columnChanges.getColumns()) {
        Column column = DatasetJsonConversion.columnModelToDatasetColumn(columnModel, EMPTY_LIST);
        bigQueryDatasetPdao.createColumn(bigQueryProject, bigQuery, table, column);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetDao.retrieve(datasetId);

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    for (var columnChanges : updateModel.getChanges().getAddColumns()) {
      DatasetTable table = dataset.getTableByName(columnChanges.getTableName()).orElseThrow();
      for (ColumnModel columnModel : columnChanges.getColumns()) {
        bigQueryDatasetPdao.deleteColumn(bigQueryProject, bigQuery, table, columnModel.getName());
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
