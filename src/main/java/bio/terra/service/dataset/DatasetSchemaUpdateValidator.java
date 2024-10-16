package bio.terra.service.dataset;

import bio.terra.common.ValidationUtils;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.flight.update.DatasetSchemaUpdateUtils;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class DatasetSchemaUpdateValidator implements Validator {

  @Autowired private DatasetRequestValidator datasetRequestValidator;

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  private void validateDatasetSchemaUpdate(DatasetSchemaUpdateModel updateModel, Errors errors) {
    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      DatasetRequestValidator.SchemaValidationContext context =
          new DatasetRequestValidator.SchemaValidationContext();
      for (TableModel tableModel : updateModel.getChanges().getAddTables()) {
        datasetRequestValidator.validateTable(tableModel, errors, context);
      }
      List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
      Object[] duplicateTables =
          newTableNames.stream()
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
              .entrySet()
              .stream()
              .filter(e -> e.getValue() > 1L)
              .map(Map.Entry::getKey)
              .toArray();
      if (duplicateTables.length > 0) {
        errors.rejectValue(
            "changes.addTables",
            "DuplicateTableNames",
            duplicateTables,
            "Cannot add multiple tables of the same name");
      }
    }

    if (DatasetSchemaUpdateUtils.hasColumnAdditions(updateModel)) {
      Object[] requiredColumns =
          updateModel.getChanges().getAddColumns().stream()
              .flatMap(c -> c.getColumns().stream())
              .filter(c -> Objects.requireNonNullElse(c.isRequired(), false))
              .map(ColumnModel::getName)
              .toArray();
      if (requiredColumns.length > 0) {
        errors.rejectValue(
            "changes.addColumns",
            "RequiredColumns",
            requiredColumns,
            "Cannot add required columns to existing tables");
      }
    }

    if (DatasetSchemaUpdateUtils.hasRelationshipAdditions(updateModel)) {
      List<RelationshipModel> newRelationships = updateModel.getChanges().getAddRelationships();
      List<String> relationshipNames =
          newRelationships.stream().map(RelationshipModel::getName).collect(Collectors.toList());
      List<String> duplicateRelationships = ValidationUtils.findDuplicates(relationshipNames);
      if (!duplicateRelationships.isEmpty()) {
        errors.rejectValue(
            "changes.addRelationships",
            "DuplicateRelationshipNames",
            "Cannot add multiple relationships of the same name: "
                + String.join(",", duplicateRelationships));
      }
    }
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target instanceof DatasetSchemaUpdateModel) {
      DatasetSchemaUpdateModel schemaUpdateRequest = (DatasetSchemaUpdateModel) target;
      validateDatasetSchemaUpdate(schemaUpdateRequest, errors);
    }
  }
}
