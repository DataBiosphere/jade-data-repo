package bio.terra.service.dataset;

import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;

@Component
public class DataDeletionRequestValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    private void validateFileSpec(DataDeletionTableModel fileSpec, Errors errors) {
        String tableName = fileSpec.getTableName();
        if (tableName == null || tableName.equals("")) {
            errors.rejectValue("tables.tableName", "TableNameMissing",
                "Requires a table name");
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
