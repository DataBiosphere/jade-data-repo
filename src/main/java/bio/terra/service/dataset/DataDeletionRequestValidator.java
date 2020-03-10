package bio.terra.service.dataset;

import bio.terra.common.ValidationUtils;
import bio.terra.model.DataDeletionFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.FileLoadModel;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Component
public class DataDeletionRequestValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    private void validateFileSpec(DataDeletionFileModel fileSpec, Errors errors) {
        fileSpec.getFileType()
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
        if (target instanceof DataDeletionRequest) {
            DataDeletionRequest dataDeletionRequest = (DataDeletionRequest) target;

            DataDeletionRequest.SpecTypeEnum specType = dataDeletionRequest.getSpecType();
            if (specType == null) {
                errors.rejectValue("specType", "SpecTypeRequired");
            } else if (!specType.equals(DataDeletionRequest.SpecTypeEnum.FILE)) {
                errors.rejectValue("specType", "OnlySpecTypeFileImplemented");
            } else {
                validateFileSpec(dataDeletionRequest.getFileSpec(), errors);
            }
        }
    }
}
