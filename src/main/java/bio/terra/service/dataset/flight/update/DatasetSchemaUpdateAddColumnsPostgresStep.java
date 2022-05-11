package bio.terra.service.dataset.flight.update;

import bio.terra.common.Column;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateAddColumnsPostgresStep implements Step {
  private final DatasetTableDao datasetTableDao;
  private final UUID datasetId;
  private final DatasetSchemaUpdateModel updateModel;

  // We need to provide a list of primary key names to the column creation method, but no new
  // primary keys can be added during column updates, so we just provide an empty list.
  private static final List<String> EMPTY_PK_LIST = Collections.emptyList();

  public DatasetSchemaUpdateAddColumnsPostgresStep(
      DatasetTableDao datasetTableDao, UUID datasetId, DatasetSchemaUpdateModel updateModel) {
    this.datasetTableDao = datasetTableDao;
    this.datasetId = datasetId;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<DatasetSchemaColumnUpdateModel> addColumns = updateModel.getChanges().getAddColumns();
    Map<String, DatasetTable> datasetTables = getDatasetTableMap();

    for (var columnAddition : addColumns) {
      DatasetTable table = datasetTables.get(columnAddition.getTableName());
      List<Column> columns =
          columnAddition.getColumns().stream()
              .map(c -> DatasetJsonConversion.columnModelToDatasetColumn(c, EMPTY_PK_LIST))
              .collect(Collectors.toList());
      datasetTableDao.createColumnsExistingTable(table.getId(), columns);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    List<DatasetSchemaColumnUpdateModel> addColumns = updateModel.getChanges().getAddColumns();
    Map<String, DatasetTable> datasetTables = getDatasetTableMap();
    for (var columnAddition : addColumns) {
      DatasetTable table = datasetTables.get(columnAddition.getTableName());
      Set<String> columnAdditionNames =
          columnAddition.getColumns().stream()
              .map(ColumnModel::getName)
              .collect(Collectors.toSet());
      List<Column> columnsToRemove =
          table.getColumns().stream()
              .filter(c -> columnAdditionNames.contains(c.getName()))
              .collect(Collectors.toList());
      datasetTableDao.removeColumns(table, columnsToRemove);
    }
    return StepResult.getStepResultSuccess();
  }

  private Map<String, DatasetTable> getDatasetTableMap() {
    return datasetTableDao.retrieveTables(datasetId).stream()
        .collect(Collectors.toMap(DatasetTable::getName, Function.identity()));
  }
}
