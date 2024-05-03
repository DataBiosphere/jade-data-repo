package bio.terra.service.dataset;

import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class DataDeletionRequestValidator implements Validator {

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

  private void validateDataDeletionRequest(DataDeletionRequest dataDeletionRequest, Errors errors) {
    DataDeletionRequest.SpecTypeEnum specType = dataDeletionRequest.getSpecType();
    if (specType == null) {
      return;
    }

    List<DataDeletionTableModel> tables = dataDeletionRequest.getTables();
    // Check the table names
    for (int i = 0; i < tables.size(); i++) {
      DataDeletionTableModel table = tables.get(i);

      if (StringUtils.isEmpty(table.getTableName())) {
        errors.rejectValue(
            String.format("tables[%d].tableName", i),
            "dataDeletionRequest.tables.name.empty",
            "Must provide a table name");
      }

      switch (dataDeletionRequest.getSpecType()) {
        case GCSFILE:
          validateFileSpec(table, i, errors);
          break;
        case JSONARRAY:
          validateJsonArraySpec(table, i, errors);
          break;
        default:
          errors.rejectValue("specType", "specType.invalid", "Invalid spec type provided");
      }
    }
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target instanceof DataDeletionRequest) {
      DataDeletionRequest dataDeletionRequest = (DataDeletionRequest) target;
      validateDataDeletionRequest(dataDeletionRequest, errors);
    }
  }
}
