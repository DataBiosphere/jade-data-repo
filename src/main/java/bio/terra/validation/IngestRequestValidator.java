package bio.terra.validation;

import bio.terra.model.IngestRequestModel;
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
    public void validate(@NotNull Object target, Errors errors) {
        if (target != null && target instanceof IngestRequestModel) {
            IngestRequestModel ingestRequest = (IngestRequestModel) target;
            validateTableName(ingestRequest.getTable(), errors);
            // TODO: validate format?
        }
    }
}
