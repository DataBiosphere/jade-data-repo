package bio.terra.service.resourcemanagement.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.google.cloud.storage.StorageClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class UpdateBucketAutoclassStepTest {
  @Mock private GoogleBucketService googleBucketService;
  @Mock private FlightContext flightContext;

  private static final String BUCKET_NAME = "bucket";

  private UpdateBucketAutoclassStep step;
  private GoogleBucketResource bucketResource;
  private FlightMap workingMap;

  @BeforeEach
  void setup() {
    step = new UpdateBucketAutoclassStep(googleBucketService);
  }

  void setBucketInfo(
      boolean autoclassEnabled, StorageClass storageClass, StorageClass terminalStorageClass) {
    bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .storageClass(storageClass)
            .autoclassEnabled(autoclassEnabled)
            .terminalStorageClass(terminalStorageClass);

    workingMap = new FlightMap();
    workingMap.put(FileMapKeys.BUCKET_INFO, bucketResource);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    boolean autoclassEnabled = true;
    StorageClass storageClass = StorageClass.REGIONAL;
    StorageClass terminalStorageClass = StorageClass.NEARLINE;
    setBucketInfo(autoclassEnabled, storageClass, terminalStorageClass);
    GoogleBucketResource bucketResourceGet =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

    step.doStep(flightContext);
    verify(googleBucketService).setBucketAutoclassToArchive(any());
    step.undoStep(flightContext);
    verify(googleBucketService)
        .setBucketAutoclass(
            any(), eq(autoclassEnabled), eq(storageClass), eq(terminalStorageClass));
    // Ensure that the bucketResource contents are the same since `any()` is used
    assertThat(bucketResourceGet, samePropertyValuesAs(bucketResource));
  }

  @Test
  void testDoAndUndoStepAutoclassDisabled() throws InterruptedException {
    boolean autoclassEnabled = false;
    StorageClass storageClass = StorageClass.STANDARD;
    StorageClass terminalStorageClass = null;
    setBucketInfo(autoclassEnabled, storageClass, terminalStorageClass);
    GoogleBucketResource bucketResourceGet =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

    step.doStep(flightContext);
    verify(googleBucketService).setBucketAutoclassToArchive(any());
    step.undoStep(flightContext);
    verify(googleBucketService)
        .setBucketAutoclass(
            any(), eq(autoclassEnabled), eq(storageClass), eq(terminalStorageClass));
    // Ensure that the bucketResource contents are the same since `any()` is used
    assertThat(bucketResourceGet, samePropertyValuesAs(bucketResource));
  }
}
