package bio.terra.service.resourcemanagement.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo.Autoclass;
import com.google.cloud.storage.StorageClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class RecordBucketAutoclassStepTest {
  @Mock private GoogleBucketService googleBucketService;
  @Mock private FlightContext flightContext;
  private FlightMap workingMap;

  private static final String BUCKET_NAME = "bucket";

  private RecordBucketAutoclassStep step;

  @BeforeEach
  void setup() {
    step = new RecordBucketAutoclassStep(googleBucketService, BUCKET_NAME);

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    Bucket bucket = mock(Bucket.class);
    GoogleBucketResource bucketResource = new GoogleBucketResource().name(BUCKET_NAME);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    when(bucket.getAutoclass()).thenReturn(Autoclass.newBuilder().setEnabled(false).build());
    when(googleBucketService.getBucketMetadata(BUCKET_NAME)).thenReturn(bucketResource);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(googleBucketService).getCloudBucket(BUCKET_NAME);
    verify(googleBucketService).getBucketMetadata(BUCKET_NAME);
    assertThat(
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class),
        samePropertyValuesAs(bucketResource));

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepBucketNotFound() throws InterruptedException {
    String errorMessage = "Bucket not found";
    when(googleBucketService.getCloudBucket(BUCKET_NAME))
        .thenThrow(new GoogleResourceException(errorMessage));

    GoogleResourceException thrown =
        assertThrows(GoogleResourceException.class, () -> step.doStep(flightContext));
    assertEquals(thrown.getMessage(), errorMessage);
  }

  @Test
  void testDoStepBucketResourceNotFound() throws InterruptedException {
    Bucket bucket = mock(Bucket.class);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    String errorMessage = "Bucket resource not found";
    when(googleBucketService.getBucketMetadata(BUCKET_NAME))
        .thenThrow(new GoogleResourceException(errorMessage));

    GoogleResourceException thrown =
        assertThrows(GoogleResourceException.class, () -> step.doStep(flightContext));
    assertEquals(thrown.getMessage(), errorMessage);
  }

  @Test
  void testDoStepBucketAutoclassAlreadySetToNearline() throws InterruptedException {
    Bucket bucket = mock(Bucket.class);
    GoogleBucketResource bucketResource =
        new GoogleBucketResource().name(BUCKET_NAME).autoclassEnabled(true);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    when(bucket.getAutoclass())
        .thenReturn(
            Autoclass.newBuilder()
                .setEnabled(true)
                .setTerminalStorageClass(StorageClass.NEARLINE)
                .build());
    when(googleBucketService.getBucketMetadata(BUCKET_NAME)).thenReturn(bucketResource);

    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class),
        samePropertyValuesAs(bucketResource));
  }

  @Test
  void testDoStepBucketAutoclassAlreadySetToArchive() throws InterruptedException {
    Bucket bucket = mock(Bucket.class);
    GoogleBucketResource bucketResource =
        new GoogleBucketResource().name(BUCKET_NAME).autoclassEnabled(true);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    when(bucket.getAutoclass())
        .thenReturn(
            Autoclass.newBuilder()
                .setEnabled(true)
                .setTerminalStorageClass(StorageClass.ARCHIVE)
                .build());
    when(googleBucketService.getBucketMetadata(BUCKET_NAME)).thenReturn(bucketResource);

    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
  }

  @Test
  void testDoStepBucketAutoclassMismatchInMetadata1() throws InterruptedException {
    // The bucket resource has autoclass disabled, but the bucket has autoclass enabled.
    Bucket bucket = mock(Bucket.class);
    GoogleBucketResource bucketResource =
        new GoogleBucketResource().name(BUCKET_NAME).autoclassEnabled(false);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    when(bucket.getAutoclass())
        .thenReturn(
            Autoclass.newBuilder()
                .setEnabled(true)
                .setTerminalStorageClass(StorageClass.ARCHIVE)
                .build());
    when(googleBucketService.getBucketMetadata(BUCKET_NAME)).thenReturn(bucketResource);

    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
  }

  @Test
  void testDoStepBucketAutoclassMismatchInMetadata2() throws InterruptedException {
    // The bucket resource has autoclass enabled, but the bucket has autoclass disabled.
    Bucket bucket = mock(Bucket.class);
    GoogleBucketResource bucketResource =
        new GoogleBucketResource().name(BUCKET_NAME).autoclassEnabled(true);
    when(googleBucketService.getCloudBucket(BUCKET_NAME)).thenReturn(bucket);
    when(bucket.getAutoclass()).thenReturn(Autoclass.newBuilder().setEnabled(false).build());
    when(googleBucketService.getBucketMetadata(BUCKET_NAME)).thenReturn(bucketResource);

    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
  }
}
