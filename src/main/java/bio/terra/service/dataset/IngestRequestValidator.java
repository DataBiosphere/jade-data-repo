package bio.terra.service.dataset;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class IngestRequestValidator implements Validator {

  private final DatasetService datasetService;

  @Autowired
  IngestRequestValidator(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  private void validateTableName(String name, Errors errors) {
    if (name == null) {
      errors.rejectValue("name", "TableNameMissing", "Ingest requires a table name");
    }
  }

  public List<String> validateIngestParams(IngestRequestModel ingestRequestModel, UUID datasetId) {
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper platform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    if (platform.isAzure()) {
      return validateAzureIngestParams(ingestRequestModel);
    }
    return Collections.emptyList();
  }

  private List<String> validateAzureIngestParams(IngestRequestModel ingestRequest) {
    List<String> errors = new ArrayList<>();
    if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.CSV) {
      // validate CSV parameters
      if (ingestRequest.getCsvSkipLeadingRows() == null) {
        errors.add("For CSV ingests, 'csvSkipLeadingRows' must be defined.");
      }
      if (ingestRequest.isCsvAllowQuotedNewlines() != null) {
        errors.add(
            "Azure CSV ingests do not support 'csvAllowQuotedNewlines', which should be left undefined.");
      }
      if (ingestRequest.getCsvNullMarker() != null) {
        errors.add(
            "Azure CSV ingest do not support 'csvNullMarker', which should be left undefined.");
      }
    }
    return errors;
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
