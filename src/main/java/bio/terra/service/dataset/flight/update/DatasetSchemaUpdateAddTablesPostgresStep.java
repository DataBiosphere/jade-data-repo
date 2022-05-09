package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateAddTablesPostgresStep implements Step {
  private final DatasetTableDao datasetTableDao;
  private final UUID datasetId;
  private final DatasetSchemaUpdateModel updateModel;

  public DatasetSchemaUpdateAddTablesPostgresStep(
      DatasetTableDao datasetTableDao, UUID datasetId, DatasetSchemaUpdateModel updateModel) {
    this.datasetTableDao = datasetTableDao;
    this.datasetId = datasetId;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<DatasetTable> datasetTables =
        updateModel.getChanges().getAddTables().stream()
            .map(DatasetJsonConversion::tableModelToTable)
            .collect(Collectors.toList());
    DatasetUtils.fillGeneratedTableNames(datasetTables);
    try {
      datasetTableDao.createTables(datasetId, datasetTables);
    } catch (IOException e) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetSchemaUpdateException("Failed to add tables to the dataset", e));
    }
    return null;
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    datasetTableDao.removeTables(datasetId, DatasetSchemaUpdateUtils.getNewTableNames(updateModel));
    return StepResult.getStepResultSuccess();
  }
}
