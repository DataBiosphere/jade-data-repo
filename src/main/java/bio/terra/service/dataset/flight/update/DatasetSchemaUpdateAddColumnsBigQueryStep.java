package bio.terra.service.dataset.flight.update;

import bio.terra.common.Column;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.bigquery.BigQuery;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;

public class DatasetSchemaUpdateAddColumnsBigQueryStep implements Step {
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetDao datasetDao;
  private final UUID datasetId;
  private final DatasetSchemaUpdateModel updateModel;

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
    String datasetName = BigQueryPdao.prefixName(dataset.getName());

    for (var columnChanges : updateModel.getChanges().getAddColumns()) {
      DatasetTable table = dataset.getTableByName(columnChanges.getTableName()).orElseThrow();
      for (ColumnModel columnModel : columnChanges.getColumns()) {
        Column column = DatasetJsonConversion.columnModelToDatasetColumn(columnModel, List.of());
        bigQueryDatasetPdao.createColumn(datasetName, bigQuery, table, column);
      }
      bigQuery.update(
          BigQueryDatasetPdao.buildLiveView(bigQueryProject.getProjectId(), datasetName, table));
    }

    DatasetSpecificationModel updatedSchema =
        DatasetJsonConversion.datasetSpecificationModelFromDatasetSchema(dataset);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), updatedSchema);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.OK);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetDao.retrieve(datasetId);

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();
    String datasetName = BigQueryPdao.prefixName(dataset.getName());

    for (var columnChanges : updateModel.getChanges().getAddColumns()) {
      DatasetTable table = dataset.getTableByName(columnChanges.getTableName()).orElseThrow();
      Set<String> columnNames =
          columnChanges.getColumns().stream().map(ColumnModel::getName).collect(Collectors.toSet());
      table.columns(
          table.getColumns().stream()
              .filter(c -> !columnNames.contains(c.getName()))
              .collect(Collectors.toList()));

      // We can't actually drop columns from a BigQuery table. All we can do is update the live view
      // to exclude deleted columns.
      //
      // The other option would be to:
      // 1. copy the table contents to a scratch table
      // 2. delete the original table
      // 3. select from the scratch table into a new table with the same name as the original
      // table, excluding the columns to be deleted.
      //
      // This may be worth it for a future iteration, but I'm not sure putting that much effort
      // into an 'undo' step is worth it for the initial implementation
      bigQuery.update(
          BigQueryDatasetPdao.buildLiveView(bigQueryProject.getProjectId(), datasetName, table));
    }
    return StepResult.getStepResultSuccess();
  }
}
