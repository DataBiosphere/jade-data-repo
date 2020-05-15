package bio.terra.service.snapshot;

import bio.terra.common.ValidationUtils;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * This validator runs along with the constraint validation that comes from the Models generated by swagger-codegen.
 * The constraints will be able to handle things like nulls, but not things like uniqueness or other structural
 * validations.
 *
 * There are a lot of null checks here because this will still be called even if a constraint validation failed.
 * Spring will not shortcut out early if a validation fails, so this Validator will still get nulls and should only
 * validate if the field is present.
 */
@Component
public class SnapshotRequestValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }


    private void validateSnapshotName(String snapshotName, Errors errors) {
        if (snapshotName == null) {
            errors.rejectValue("name", "SnapshotNameMissing");
        }
    }

    private void validateSnapshotContents(List<SnapshotRequestContentsModel> contentsList, Errors errors) {
        if (contentsList == null || contentsList.isEmpty()) {
            errors.rejectValue("contents", "SnapshotSourceListEmpty");
        } else {
            contentsList.forEach(contents -> {
                String datasetName = contents.getDatasetName();
                if (datasetName == null) {
                    errors.rejectValue("contents", "SnapshotDatasetNameMissing");
                }

                switch (contents.getMode()) {
                    case BYASSET:
                        validateSnapshotAssetSpec(contents.getAssetSpec(), errors);
                        break;
                    case BYLIVEVIEW:
                        // no additional validation necessary
                        break;
                    case BYQUERY:
                        validateSnapshotQuerySpec(contents.getQuerySpec(), errors);
                        break;
                    case BYROWID:
                        validateSnapshotRowIdSpec(contents.getRowIdSpec(), errors);
                        break;
                    default:
                        errors.rejectValue("contents", "SnapshotContentsModeInvalid");
                }
            });
        }
    }

    private void validateSnapshotAssetSpec(SnapshotRequestAssetModel assetModel, Errors errors) {
        List<String> rootValues = assetModel.getRootValues();
        if (rootValues == null || rootValues.isEmpty()) {
            errors.rejectValue("contents", "SnapshotRootValuesListEmpty");
        }
        String assetName = assetModel.getAssetName();
        if (assetName == null) {
            errors.rejectValue("contents", "SnapshotAssetNameMissing");
        }
    }

    private void validateSnapshotLiveViewSpec() {

    }

    private void validateSnapshotQuerySpec(SnapshotRequestQueryModel queryModel, Errors errors) {
        String query = queryModel.getQuery();
        if (query == null) {
            errors.rejectValue("contents", "SnapshotQueryEmpty");
        }
        // TODO add addtional ANTLR validation?
        String assetName = queryModel.getAssetName();
        if (assetName == null) {
            errors.rejectValue("contents", "SnapshotAssetNameMissing");
        }
    }

    private void validateSnapshotRowIdSpec(SnapshotRequestRowIdModel rowIdSpec, Errors errors) {
        List<SnapshotRequestRowIdTableModel> tables = rowIdSpec.getTables();
        if (tables == null || tables.isEmpty()) {
            errors.rejectValue("contents", "SnapshotTablesListEmpty");
        } else {
            tables.forEach(t -> {
                if (StringUtils.isBlank(t.getTableName())) {
                    errors.rejectValue("contents", "SnapshotTableNameMissing");
                }
                List<String> columns = t.getColumns();
                if (columns == null || columns.isEmpty()) {
                    errors.rejectValue("contents", "SnapshotTableColumnsMissing");
                }
                List<String> rowIds = t.getRowIds();
                if (rowIds == null || rowIds.isEmpty()) {
                    errors.rejectValue("contents", "SnapshotTableRowIdsMissing");
                }
            });
        }
    }

    private void validateSnapshotDescription(String description, Errors errors) {
        if (description == null) {
            errors.rejectValue("description", "SnapshotDescriptionMissing");
        } else if (!ValidationUtils.isValidDescription(description)) {
            errors.rejectValue("description", "SnapshotDescriptionTooLong");
        }
    }

    private void validateSnapshotProfileId(String profileId, Errors errors) {
        if (profileId == null) {
            errors.rejectValue("profileId", "SnapshotMissingProfileId");
        }
    }

    @Override
    public void validate(@NotNull Object target, Errors errors) {
        if (target != null && target instanceof SnapshotRequestModel) {
            SnapshotRequestModel snapshotRequestModel = (SnapshotRequestModel) target;
            validateSnapshotName(snapshotRequestModel.getName(), errors);
            validateSnapshotProfileId(snapshotRequestModel.getProfileId(), errors);
            validateSnapshotDescription(snapshotRequestModel.getDescription(), errors);
            validateSnapshotContents(snapshotRequestModel.getContents(), errors);
        }
    }
}
