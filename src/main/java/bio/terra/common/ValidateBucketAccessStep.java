package bio.terra.common;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate that any GCS files specified directly in a flight request are readable by the caller.
 * This spans a security hole for datasets which use the generic TDR service account for file
 * operations: if the generic TDR SA has access to a file but the caller initiating the flight does
 * not, we should fail the flight.
 *
 * <p>GCS files specified indirectly -- i.e. referenced in a control file -- do not have their
 * access validated in this step.
 *
 * <p>This step should be added to flights when GCS files could be specified directly in the
 * request, which could include instances where the destination dataset is backed by Azure.
 */
public class ValidateBucketAccessStep extends DefaultUndoStep {
  private static final Logger logger = LoggerFactory.getLogger(ValidateBucketAccessStep.class);

  private final CloudFileReader cloudFileReader;
  private final AuthenticatedUserRequest userRequest;
  private final Dataset dataset;

  public ValidateBucketAccessStep(
      CloudFileReader cloudFileReader, AuthenticatedUserRequest userRequest, Dataset dataset) {
    this.cloudFileReader = cloudFileReader;
    this.userRequest = userRequest;
    this.dataset = dataset;
  }

  // Expected substring in Google storage exception message when delays in pet account access
  // propagation may impact our ability to verify access
  @VisibleForTesting
  static final String PET_PROPAGATION_ERROR_MSG =
      "does not have serviceusage.services.use access to the Google Cloud project.";

  @Override
  @SuppressFBWarnings(
      value = "BC",
      justification =
          "The check is wrong. The exception when calling validateUserCanRead can in "
              + "fact be a GoogleJsonResponseException")
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    List<String> sourcePaths;
    Object loadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), Object.class);
    if (loadModel instanceof FileLoadModel fileLoadModel) {
      // Single file ingest
      sourcePaths = List.of(fileLoadModel.getSourcePath());
    } else if (loadModel instanceof BulkLoadRequestModel bulkLoadRequestModel) {
      // Bulk file ingest (JSON format)
      sourcePaths = List.of(bulkLoadRequestModel.getLoadControlFile());
    } else if (loadModel instanceof BulkLoadArrayRequestModel bulkLoadArrayRequestModel) {
      // Bulk file ingest (array format)
      sourcePaths =
          bulkLoadArrayRequestModel.getLoadArray().stream()
              .map(BulkLoadFileModel::getSourcePath)
              .toList();
    } else if (loadModel instanceof IngestRequestModel ingestRequestModel) {
      // Combined metadata and file ingest
      if (ingestRequestModel.getFormat().equals(FormatEnum.ARRAY)) {
        // Array format: TDR will write its own control file in a scratch bucket. The user
        // initiating the flight is not expected to have access to it.
        sourcePaths = List.of();
      } else {
        // JSON or CSV format: User-supplied control file
        sourcePaths = List.of(ingestRequestModel.getPath());
      }
    } else if (loadModel instanceof DataDeletionRequest dataDeletionRequest) {
      // Soft deletes
      sourcePaths =
          dataDeletionRequest.getTables().stream().map(t -> t.getGcsFileSpec().getPath()).toList();
    } else {
      throw new IllegalArgumentException("Invalid request type");
    }
    // The destination dataset may be Azure-based, and thus would not have a Google project ID
    String cloudEncapsulationId =
        Optional.ofNullable(dataset.getProjectResource())
            .map(GoogleProjectResource::getGoogleProjectId)
            .orElse(null);
    try {
      cloudFileReader.validateUserCanRead(sourcePaths, cloudEncapsulationId, userRequest, dataset);
    } catch (StorageException e) {
      if (e.getCode() == HttpStatus.SC_FORBIDDEN
          && e.getMessage().contains(PET_PROPAGATION_ERROR_MSG)) {
        logger.warn("Pet service account has not propagated permissions yet", e);
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      }
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
    return StepResult.getStepResultSuccess();
  }
}
