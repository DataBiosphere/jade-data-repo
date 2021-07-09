package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.model.DataDeletionGcsFileModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DataDeletionTableModel;
import bio.terra.datarepo.service.dataset.exception.InvalidUriException;
import bio.terra.datarepo.service.dataset.flight.ingest.IngestUtils;
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
        IngestUtils.parseBlobUri(gcsFileSpec.getPath());
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
