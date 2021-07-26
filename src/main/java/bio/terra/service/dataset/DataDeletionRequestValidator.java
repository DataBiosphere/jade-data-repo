package bio.terra.service.dataset;

import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.exception.InvalidUriException;
import javax.validation.constraints.NotNull;
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

  private void validateFileSpec(DataDeletionTableModel fileSpec, Errors errors) {
    String tableName = fileSpec.getTableName();
    if (StringUtils.isEmpty(tableName)) {
      errors.rejectValue("tables.tableName", "TableNameMissing", "Requires a table name");
    }

    DataDeletionGcsFileModel gcsFileSpec = fileSpec.getGcsFileSpec();
    if (gcsFileSpec == null) {
      errors.rejectValue("tables.gcsFileSpec", "FileSpecMissing", "Requires a file spec");
    } else {
      try {
        GcsUriUtils.parseBlobUri(gcsFileSpec.getPath());
      } catch (InvalidUriException ex) {
        errors.rejectValue("tables.gcsFileSpec.path", "InvalidGsUri", ex.getMessage());
      }
    }
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target instanceof DataDeletionRequest) {
      DataDeletionRequest dataDeletionRequest = (DataDeletionRequest) target;
      dataDeletionRequest.getTables().forEach(table -> validateFileSpec(table, errors));
    }
  }
}
