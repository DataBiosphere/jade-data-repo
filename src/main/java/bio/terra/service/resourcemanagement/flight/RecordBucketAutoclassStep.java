package bio.terra.service.resourcemanagement.flight;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo.Autoclass;
import com.google.cloud.storage.StorageClass;

public class RecordBucketAutoclassStep implements Step {

  private final GoogleBucketService googleBucketService;
  private final String bucketName;

  public RecordBucketAutoclassStep(GoogleBucketService googleBucketService, String bucketName) {
    this.googleBucketService = googleBucketService;
    this.bucketName = bucketName;
  }

  private StepResult stepResultFailure(String message) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new GoogleResourceException(message));
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource = googleBucketService.getBucketMetadata(bucketName);
    Bucket bucket = googleBucketService.getCloudBucket(bucketName);
    bucketResource = bucketResource.storageClass(bucket.getStorageClass());
    Autoclass autoclass = bucket.getAutoclass();
    if (autoclass != null) {
      if (autoclass.getEnabled() && autoclass.getTerminalStorageClass() == StorageClass.ARCHIVE) {
        return stepResultFailure("Bucket autoclass already set to ARCHIVE: " + bucketName);
      }
      if (autoclass.getEnabled() != bucketResource.getAutoclassEnabled()) {
        return stepResultFailure("Bucket autoclass mismatch in metadata: " + bucketName);
      }
      if (autoclass.getEnabled()) {
        bucketResource = bucketResource.terminalStorageClass(autoclass.getTerminalStorageClass());
      }
    }
    workingMap.put(FileMapKeys.BUCKET_INFO, bucketResource);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
