package bio.terra.common;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.stream.Collectors;

public class ValidateBucketAccessStep implements Step {
  private final GcsPdao gcsPdao;
  private final AuthenticatedUserRequest userRequest;

  public ValidateBucketAccessStep(GcsPdao gcsPdao, AuthenticatedUserRequest userRequest) {
    this.gcsPdao = gcsPdao;
    this.userRequest = userRequest;
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
      sourcePath = List.of(((IngestRequestModel) loadModel).getPath());
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
    gcsPdao.validateUserCanRead(sourcePath, userRequest);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
