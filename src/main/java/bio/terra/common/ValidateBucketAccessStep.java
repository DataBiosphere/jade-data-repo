package bio.terra.common;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ValidateBucketAccessStep implements Step {
  private final GcsPdao gcsPdao;
  private final AuthenticatedUserRequest userRequest;
  private final String projectId;

  public ValidateBucketAccessStep(
      GcsPdao gcsPdao, String projectId, AuthenticatedUserRequest userRequest) {
    this.gcsPdao = gcsPdao;
    this.projectId = projectId;
    this.userRequest = userRequest;
  }

  public ValidateBucketAccessStep(
      GcsPdao gcsPdao,
      UUID datasetId,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest) {
    this.gcsPdao = gcsPdao;
    this.userRequest = userRequest;
    this.projectId =
        datasetService.retrieveAvailable(datasetId).getProjectResource().getGoogleProjectId();
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    List<String> sourcePath;
    Object loadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), Object.class);
    if (loadModel instanceof FileLoadModel) {
      // Single file ingest
      sourcePath = List.of(((FileLoadModel) loadModel).getSourcePath());
    } else if (loadModel instanceof BulkLoadRequestModel) {
      // Bulk file ingest
      sourcePath = List.of(((BulkLoadRequestModel) loadModel).getLoadControlFile());
    } else if (loadModel instanceof IngestRequestModel) {
      // Metadata ingests
      // Don't validate if we are ingesting as a payload object
      if (((IngestRequestModel) loadModel).getFormat().equals(FormatEnum.ARRAY)) {
        sourcePath = List.of();
      } else {
        sourcePath = List.of(((IngestRequestModel) loadModel).getPath());
      }
    } else if (loadModel instanceof DataDeletionRequest) {
      // Soft deletes
      sourcePath =
          ((DataDeletionRequest) loadModel)
              .getTables().stream()
                  .map(t -> t.getGcsFileSpec().getPath())
                  .collect(Collectors.toList());
    } else {
      throw new IllegalArgumentException("Invalid request type");
    }
    gcsPdao.validateUserCanRead(sourcePath, projectId, userRequest);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
