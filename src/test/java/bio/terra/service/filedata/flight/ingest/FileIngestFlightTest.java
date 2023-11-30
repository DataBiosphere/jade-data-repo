package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightTestUtils.mockFlightAppConfigSetup;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.model.CloudPlatform;
import bio.terra.model.FileLoadModel;
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
@Tag("bio.terra.common.category.Unit")
public class FileIngestFlightTest {
  @Mock private ApplicationContext context;
  @Mock private DatasetSummary datasetSummary;
  private FlightMap inputParameters;

  @BeforeEach
  void beforeEach() {
    UUID datasetId = UUID.randomUUID();

    DatasetService datasetService = mock(DatasetService.class);
    when(datasetService.retrieve(datasetId)).thenReturn(new Dataset(datasetSummary));

    mockFlightAppConfigSetup(context);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), datasetId.toString());
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new FileLoadModel());
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testFileIngestFlight(CloudPlatform cloudPlatform) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var flight = new FileIngestFlight(inputParameters, context);
    assertThat(
        "File ingest flight locks "
            + cloudPlatform
            + " dataset, then performs file access validation, then unlocks dataset",
        FlightTestUtils.getStepNames(flight),
        containsInRelativeOrder(
            "LockDatasetStep", "ValidateBucketAccessStep", "UnlockDatasetStep"));

    LockDatasetStep lockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, LockDatasetStep.class);
    assertThat(
        "File ingest flight obtains shared dataset lock", lockDatasetStep.isSharedLock(), is(true));
    assertThat(
        "Dataset lock step does not suppress 'dataset not found' exceptions",
        lockDatasetStep.shouldSuppressNotFoundException(),
        is(false));

    UnlockDatasetStep unlockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, UnlockDatasetStep.class);
    assertThat(
        "File ingest flight removes shared dataset lock",
        unlockDatasetStep.isSharedLock(),
        is(true));
  }
}
