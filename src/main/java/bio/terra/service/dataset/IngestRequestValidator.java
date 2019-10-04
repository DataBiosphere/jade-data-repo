package bio.terra.service.dataset;

import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.common.ValidationUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;

@Component
public class IngestRequestValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    private void validateTableName(String name, Errors errors) {
        if (name == null) {
            errors.rejectValue("name", "TableNameMissing",
                "Ingest requires a table name");
        } else if (!ValidationUtils.isValidName(name)) {
            errors.rejectValue("name", "TableNameInvalid",
                "Invalid table name " + name);
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
