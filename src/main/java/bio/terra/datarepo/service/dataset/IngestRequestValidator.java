package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.model.FileLoadModel;
import bio.terra.datarepo.model.IngestRequestModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class IngestRequestValidator implements Validator {

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  private void validateTableName(String name, Errors errors) {
    if (name == null) {
      errors.rejectValue("name", "TableNameMissing", "Ingest requires a table name");
    }
  }

  @Override
  @SuppressFBWarnings(
      value = "UC_USELESS_VOID_METHOD",
      justification = "FB mistake - this clearly validates and returns data in errors")
  public void validate(@NotNull Object target, Errors errors) {
    if (target instanceof IngestRequestModel) {
      IngestRequestModel ingestRequest = (IngestRequestModel) target;
      validateTableName(ingestRequest.getTable(), errors);
    } else if (target instanceof FileLoadModel) {
      FileLoadModel fileLoadModel = (FileLoadModel) target;
      if (fileLoadModel.getProfileId() == null) {
        errors.rejectValue("profileId", "ProfileIdMissing", "File ingest requires a profile id.");
      }
    }
  }
}
