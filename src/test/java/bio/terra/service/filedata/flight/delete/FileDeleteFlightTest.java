package bio.terra.service.filedata.flight.delete;

import static bio.terra.common.FlightTestUtils.mockFlightAppConfigSetup;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class FileDeleteFlightTest {

  @Mock private ApplicationContext context;
  @Mock private DatasetSummary datasetSummary;
  private FlightMap inputParameters;

  @BeforeEach
  void beforeEach() {
    UUID datasetId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();

    DatasetService datasetService = mock(DatasetService.class);
    when(datasetService.retrieve(datasetId)).thenReturn(new Dataset(datasetSummary));

    mockFlightAppConfigSetup(context);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), datasetId.toString());
    inputParameters.put(JobMapKeys.FILE_ID.getKeyName(), fileId.toString());
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testFileDeleteFlight(CloudPlatform cloudPlatform) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var flight = new FileDeleteFlight(inputParameters, context);
    assertThat(
        "File delete flight locks " + cloudPlatform + " dataset, then unlocks dataset",
        FlightTestUtils.getStepNames(flight),
        containsInRelativeOrder("LockDatasetStep", "UnlockDatasetStep"));

    LockDatasetStep lockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, LockDatasetStep.class);
    assertThat(
        "File delete flight obtains shared dataset lock", lockDatasetStep.isSharedLock(), is(true));
    assertThat(
        "Dataset lock step does not suppress 'dataset not found' exceptions",
        lockDatasetStep.shouldSuppressNotFoundException(),
        is(false));

    UnlockDatasetStep unlockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, UnlockDatasetStep.class);
    assertThat(
        "File delete flight removes shared dataset lock",
        unlockDatasetStep.isSharedLock(),
        is(true));
  }
}
