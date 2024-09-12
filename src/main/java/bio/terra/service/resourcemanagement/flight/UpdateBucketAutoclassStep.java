package bio.terra.service.resourcemanagement.flight;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class UpdateBucketAutoclassStep implements Step {

  private final GoogleBucketService googleBucketService;

  public UpdateBucketAutoclassStep(GoogleBucketService googleBucketService) {
    this.googleBucketService = googleBucketService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

    googleBucketService.setBucketAutoclassToArchive(bucketResource);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

    googleBucketService.setBucketAutoclass(
        bucketResource,
        bucketResource.getAutoclassEnabled(),
        bucketResource.getStorageClass(),
        bucketResource.getTerminalStorageClass());

    return StepResult.getStepResultSuccess();
  }
}
