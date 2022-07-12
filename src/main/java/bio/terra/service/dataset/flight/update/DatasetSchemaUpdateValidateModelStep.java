package bio.terra.service.dataset.flight.update;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.ValidationUtils;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;

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
    final List<String> newTableNames;
    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
      List<String> uniqueTableNames = ListUtils.intersection(existingTableNames, newTableNames);
      if (!uniqueTableNames.isEmpty()) {
        return failsValidation(
            "Could not validate table additions",
            List.of(
                "Found new tables that would overwrite existing tables",
                String.join(", ", uniqueTableNames)));
      }
    } else {
      newTableNames = List.of();
    }

    if (DatasetSchemaUpdateUtils.hasColumnAdditions(updateModel)) {
      List<DatasetSchemaColumnUpdateModel> addColumns = updateModel.getChanges().getAddColumns();
      List<String> newAndExistingTableNames = ListUtils.union(existingTableNames, newTableNames);

      List<String> missingTables = new ArrayList<>();
      List<String> duplicateColumns = new ArrayList<>();
      for (var columnAddition : addColumns) {
        String tableName = columnAddition.getTableName();
        if (!newAndExistingTableNames.contains(tableName)) {
          missingTables.add(tableName);
          continue;
        }
        if (newTableNames.contains(tableName)) {
          duplicateColumns.addAll(conflictingNewColumns(tableName, columnAddition.getColumns()));
        } else {
          duplicateColumns.addAll(
              conflictingExistingColumns(tableName, dataset, columnAddition.getColumns()));
        }
      }
      if (!missingTables.isEmpty()) {
        return failsValidation(
            "Could not find tables to add columns to",
            List.of(String.format("Missing tables: %s", String.join(", ", missingTables))));
      }
      if (!duplicateColumns.isEmpty()) {
        return failsValidation(
            "Cannot overwrite existing or to-be-added columns in tables", duplicateColumns);
      }
    }

    if (DatasetSchemaUpdateUtils.hasRelationshipAdditions(updateModel)) {
      List<Relationship> existingRelationships = dataset.getRelationships();
      List<RelationshipModel> newRelationships = updateModel.getChanges().getAddRelationships();
      List<String> conflictingRelationshipNames =
          conflictingRelationshipNames(existingRelationships, newRelationships);
      if (!conflictingRelationshipNames.isEmpty()) {
        return failsValidation(
            "Could not validate relationship additions",
            List.of(
                "Relationships with these names already exist for this dataset: ",
                String.join(", ", conflictingRelationshipNames)));
      }

      ArrayList<TableModel> allTables =
          dataset.getTables().stream()
              .map(DatasetJsonConversion::tableModelFromTable)
              .collect(Collectors.toCollection(ArrayList::new));
      if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
        allTables.addAll(updateModel.getChanges().getAddTables());
      }

      ArrayList<String> validationErrors = new ArrayList<>();
      for (var relationship : newRelationships) {
        ArrayList<Map<String, String>> errors =
            ValidationUtils.getRelationshipValidationErrors(relationship, allTables);
        validationErrors.addAll(formatValidationErrors(errors));
      }
      if (!validationErrors.isEmpty()) {
        return failsValidation(
            "Could not validate relationship additions",
            List.of("Found invalid terms: ", String.join(", ", validationErrors)));
      }
    }
    return StepResult.getStepResultSuccess();
  }

  private List<String> conflictingRelationshipNames(
      List<Relationship> existingRelationships, List<RelationshipModel> newRelationships) {
    List<String> existingRelationshipNames =
        existingRelationships.stream().map(Relationship::getName).collect(Collectors.toList());
    List<String> newRelationshipNames =
        newRelationships.stream().map(RelationshipModel::getName).toList();
    return ListUtils.intersection(existingRelationshipNames, newRelationshipNames);
  }

  private List<String> conflictingNewColumns(String tableName, List<ColumnModel> newColumns) {
    TableModel tableModel =
        updateModel.getChanges().getAddTables().stream()
            .filter(tm -> tm.getName().equals(tableName))
            .findFirst()
            .orElseThrow();
    List<String> newTableColumnNames =
        tableModel.getColumns().stream().map(ColumnModel::getName).collect(Collectors.toList());
    List<String> newColumnNames = newColumnNames(newColumns);
    return prependTableName(tableName, ListUtils.intersection(newTableColumnNames, newColumnNames));
  }

  private List<String> conflictingExistingColumns(
      String tableName, Dataset dataset, List<ColumnModel> newColumns) {
    DatasetTable table = dataset.getTableByName(tableName).orElseThrow();
    List<String> existingColumnNames =
        table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
    List<String> newColumnNames = newColumnNames(newColumns);
    return prependTableName(tableName, ListUtils.intersection(existingColumnNames, newColumnNames));
  }

  private StepResult failsValidation(String message, List<String> reasons) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new DatasetSchemaUpdateException(message, reasons));
  }

  private List<String> formatValidationErrors(ArrayList<Map<String, String>> errors) {
    return errors.stream()
        .flatMap(
            errorMap ->
                errorMap.entrySet().stream()
                    .map(entry -> String.format("%s: %s", entry.getKey(), entry.getValue())))
        .collect(Collectors.toList());
  }

  private static List<String> newColumnNames(List<ColumnModel> columns) {
    return columns.stream().map(ColumnModel::getName).collect(Collectors.toList());
  }

  private static List<String> prependTableName(String tableName, List<String> columnNames) {
    return columnNames.stream().map(n -> tableName + ":" + n).collect(Collectors.toList());
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
