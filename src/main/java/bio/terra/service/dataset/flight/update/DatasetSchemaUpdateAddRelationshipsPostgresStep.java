package bio.terra.service.dataset.flight.update;

import bio.terra.common.Relationship;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetRelationshipDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateAddRelationshipsPostgresStep implements Step {
  private final DatasetTableDao datasetTableDao;
  private final UUID datasetId;
  private final DatasetRelationshipDao relationshipDao;
  private final DatasetSchemaUpdateModel updateModel;

  public DatasetSchemaUpdateAddRelationshipsPostgresStep(
      DatasetTableDao datasetTableDao,
      UUID datasetId,
      DatasetRelationshipDao relationshipDao,
      DatasetSchemaUpdateModel updateModel) {
    this.datasetTableDao = datasetTableDao;
    this.datasetId = datasetId;
    this.relationshipDao = relationshipDao;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Map<String, DatasetTable> tables =
        datasetTableDao.retrieveTables(datasetId).stream()
            .collect(Collectors.toMap(DatasetTable::getName, Function.identity()));
    List<Relationship> relationships =
        updateModel.getChanges().getAddRelationships().stream()
            .map(r -> DatasetJsonConversion.relationshipModelToDatasetRelationship(r, tables))
            .collect(Collectors.toList());
    try {
      relationshipDao.createDatasetRelationshipsWithTransaction(relationships);
    } catch (Exception e) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetSchemaUpdateException("Failed to add relationships to the dataset", e));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
