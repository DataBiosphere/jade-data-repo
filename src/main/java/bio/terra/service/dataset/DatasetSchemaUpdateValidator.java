package bio.terra.service.dataset;

import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.flight.update.DatasetSchemaUpdateUtils;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
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
      if (DatasetSchemaUpdateUtils.hasColumnAdditions(updateModel)) {
        Object[] requiredColumns =
            updateModel.getChanges().getAddColumns().stream()
                .flatMap(c -> c.getColumns().stream())
                .filter(c -> c.isRequired())
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
    }
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target instanceof DatasetSchemaUpdateModel) {
      DatasetSchemaUpdateModel dataDeletionRequest = (DatasetSchemaUpdateModel) target;
      validateDatasetSchemaUpdate(dataDeletionRequest, errors);
    }
  }
}
