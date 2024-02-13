package bio.terra.service.dataset;

import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
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
    if (target instanceof IngestRequestModel ingestRequest) {
      validateTableName(ingestRequest.getTable(), errors);
      boolean isPayloadIngest =
          ingestRequest.getFormat().equals(IngestRequestModel.FormatEnum.ARRAY);
      if (StringUtils.isEmpty(ingestRequest.getPath()) && !isPayloadIngest) {
        errors.rejectValue(
            "path", "PathIsMissing", "Path is required when ingesting from a cloud object");
      }

      if (!StringUtils.isEmpty(ingestRequest.getPath()) && isPayloadIngest) {
        errors.rejectValue(
            "path", "PathIsPresent", "Path should not be specified when ingesting from an array");
      }

      if (ListUtils.emptyIfNull(ingestRequest.getRecords()).isEmpty() && isPayloadIngest) {
        errors.rejectValue(
            "records", "DataPayloadIsMissing", "Records is required when ingesting as an array");
      }

      if (!ListUtils.emptyIfNull(ingestRequest.getRecords()).isEmpty() && !isPayloadIngest) {
        errors.rejectValue(
            "records",
            "DataPayloadIsPresent",
            "Records should not be specified when ingesting from a path");
      }
    } else if (target instanceof FileLoadModel fileLoadModel) {
      if (fileLoadModel.getProfileId() == null) {
        errors.rejectValue("profileId", "ProfileIdMissing", "File ingest requires a profile id.");
      }
    } else if (target instanceof BulkLoadRequestModel bulkLoadRequestModel) {
      if (bulkLoadRequestModel.isBulkMode()
          && StringUtils.isEmpty(bulkLoadRequestModel.getLoadTag())) {
        errors.rejectValue("loadTag", "MissingLoadTag", "Load tag is required for isBulkMode");
      }
    } else if (target instanceof BulkLoadArrayRequestModel bulkLoadArrayRequestModel) {
      if (bulkLoadArrayRequestModel.isBulkMode()
          && StringUtils.isEmpty(bulkLoadArrayRequestModel.getLoadTag())) {
        errors.rejectValue("loadTag", "MissingLoadTag", "Load tag is required for isBulkMode");
      }
    }
  }
}
