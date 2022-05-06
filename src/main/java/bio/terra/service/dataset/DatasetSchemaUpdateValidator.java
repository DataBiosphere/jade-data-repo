package bio.terra.service.dataset;

import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.TableModel;
import bio.terra.service.common.gcs.GcsUriUtils;
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

  private void validateFileSpec(DataDeletionTableModel table, int index, Errors errors) {
    DataDeletionGcsFileModel gcsFileSpec = table.getGcsFileSpec();
    DataDeletionJsonArrayModel jsonArraySpec = table.getJsonArraySpec();
    if (gcsFileSpec == null) {
      if (jsonArraySpec != null) {
        errors.rejectValue(
            String.format("tables[%d].jsonArraySpec", index),
            "dataDeletion.specType.mismatch",
            "JsonArray table specs provided when GcsFileSpec chosen");
      } else {
        errors.rejectValue(
            String.format("tables[%d].gcsFileSpec", index),
            "dataDeletion.tables.gcsFileSpec.missing",
            "GcsFileSpec table spec missing when GcsFileSpec chosen");
      }
    } else {
      try {
        if (gcsFileSpec.getPath() != null) {
          GcsUriUtils.validateBlobUri(gcsFileSpec.getPath());
        }
      } catch (IllegalArgumentException ex) {
        errors.rejectValue(
            String.format("tables[%d].gcsFileSpec.path", index),
            "dataDeletion.tables.gcsFileSpec.path.invalid",
            ex.getMessage());
      }
    }
  }

  private void validateJsonArraySpec(DataDeletionTableModel table, int index, Errors errors) {
    DataDeletionGcsFileModel gcsFileSpec = table.getGcsFileSpec();
    DataDeletionJsonArrayModel jsonArraySpec = table.getJsonArraySpec();

    if (jsonArraySpec == null) {
      if (gcsFileSpec != null) {
        errors.rejectValue(
            String.format("tables[%d].gcsFileSpec", index),
            "dataDeletion.specType.mismatch",
            "GcsFile table spec provided when JsonArraySpec chosen.");
      } else {
        errors.rejectValue(
            String.format("tables[%d].jsonArraySpec", index),
            "dataDeletion.tables.jsonArraySpec.missing",
            "JsonArraySpec table spec missing when JsonArraySpec chosen");
      }
    }
  }

  private void validateDatasetSchemaUpdate(DatasetSchemaUpdateModel updateModel, Errors errors) {
    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      DatasetRequestValidator.SchemaValidationContext context =
          new DatasetRequestValidator.SchemaValidationContext();
      for (TableModel tableModel : updateModel.getChanges().getAddTables()) {
        datasetRequestValidator.validateTable(tableModel, errors, context);
      }
      List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
      if (newTableNames.stream().distinct().count() != newTableNames.size()) {
        Object[] duplicateTables =
            newTableNames.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1L)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
                .toArray();
        errors.rejectValue(
            "changes.addTables",
            "DuplicateTableNames",
            duplicateTables,
            "Cannot add multiple tables of the same name");
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
