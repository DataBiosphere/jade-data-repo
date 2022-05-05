package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class DatasetSchemaUpdateValidateModelStep implements Step {
  private final UUID datasetId;
  private final DatasetService datasetService;
  private final DatasetSchemaUpdateModel updateModel;

  public DatasetSchemaUpdateValidateModelStep(
      DatasetService datasetService, UUID datasetId, DatasetSchemaUpdateModel updateModel) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    List<String> existingTableNames =
        dataset.getTables().stream().map(DatasetTable::getName).collect(Collectors.toList());
    List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
    Collection<String> uniqueTableNames =
        CollectionUtils.intersection(existingTableNames, newTableNames);

    if (!uniqueTableNames.isEmpty()) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetSchemaUpdateException(
              "Could not validate table additions",
              List.of(
                  "Found new tables that would overwrite existing tables",
                  String.join(", ", uniqueTableNames))));
    }
    if (newTableNames.stream().distinct().count() != newTableNames.size()) {
      String duplicateTables = newTableNames.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().stream().filter(e -> e.getValue() > 1L).map(Map.Entry::getKey).collect(Collectors.joining(", "));
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new DatasetSchemaUpdateException(
              "Could not validate table additions",
              List.of(
                  "Found duplicate new tables", duplicateTables)));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
