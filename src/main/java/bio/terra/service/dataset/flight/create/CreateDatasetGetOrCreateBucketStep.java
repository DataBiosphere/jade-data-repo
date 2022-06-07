package bio.terra.service.dataset.flight.create;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.flight.ingest.IngestFilePrimaryDataLocationStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetGetOrCreateBucketStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateDatasetGetOrCreateBucketStep.class);

  private final AuthenticatedUserRequest userReq;
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;
  private final IamService iamService;

  public CreateDatasetGetOrCreateBucketStep(
      AuthenticatedUserRequest userReq,
      ResourceService resourceService,
      DatasetRequestModel datasetRequestModel,
      IamService iamService) {
    this.userReq = userReq;
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
    this.iamService = iamService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    GoogleRegion region = GoogleRegion.fromValueWithDefault(datasetRequestModel.getRegion());
    UUID projectId = workingMap.get(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, UUID.class);
    GoogleProjectResource googleProjectResource = resourceService.getProjectResource(projectId);

    Callable<List<String>> getReaderGroups =
        () ->
            iamService
                .retrievePolicyEmails(userReq, IamResourceType.DATASET, datasetId)
                .entrySet()
                .stream()
                .filter(
                    entry ->
                        IngestFilePrimaryDataLocationStep.BUCKET_READER_ROLES.contains(
                            entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

    try {
      GoogleBucketResource bucketForFile =
          resourceService.getOrCreateBucketForFile(
              region, googleProjectResource, context.getFlightId(), getReaderGroups);

      workingMap.put(FileMapKeys.BUCKET_INFO, bucketForFile);
    } catch (BucketLockException blEx) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
    } catch (GoogleResourceNamingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // Leaving artifacts on undo
    return StepResult.getStepResultSuccess();
  }
}
