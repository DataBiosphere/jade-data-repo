package bio.terra.service.resourcemanagement.flight;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class UpdateDatabaseAutoclassStepTest {
  @Mock private GoogleBucketService googleBucketService;
  @Mock private FlightContext flightContext;

  private static final String BUCKET_NAME = "bucket";

  private UpdateDatabaseAutoclassStep step;
  private GoogleBucketResource bucketResource;
  private FlightMap workingMap;

  @BeforeEach
  void setup() {
    step = new UpdateDatabaseAutoclassStep(googleBucketService);
  }

  void setBucketInfo(boolean autoclassEnabled) {
    bucketResource =
        new GoogleBucketResource().name(BUCKET_NAME).autoclassEnabled(autoclassEnabled);

    workingMap = new FlightMap();
    workingMap.put(FileMapKeys.BUCKET_INFO, bucketResource);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    boolean autoclass = false;
    setBucketInfo(autoclass);
    step.doStep(flightContext);
    verify(googleBucketService).setBucketAutoclassMetadata(BUCKET_NAME, true);
    step.undoStep(flightContext);
    verify(googleBucketService).setBucketAutoclassMetadata(BUCKET_NAME, autoclass);
  }
}
