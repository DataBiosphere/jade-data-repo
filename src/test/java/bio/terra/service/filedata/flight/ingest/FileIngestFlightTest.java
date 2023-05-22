package bio.terra.service.filedata.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.FlightTestUtils;
import bio.terra.model.CloudPlatform;
import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
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

    ApplicationConfiguration appConfig = mock(ApplicationConfiguration.class);
    when(appConfig.getMaxStairwayThreads()).thenReturn(1);

    when(context.getBean(any(Class.class))).thenReturn(null);
    // Beans that are interacted with directly in flight construction rather than simply passed
    // to steps need to be added to our context mock.
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);
    when(context.getBean(ApplicationConfiguration.class)).thenReturn(appConfig);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), datasetId.toString());
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new FileLoadModel());
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testFileIngestFlightValidatesFileAccess(CloudPlatform cloudPlatform) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var flight = new FileIngestFlight(inputParameters, context);
    assertThat(
        "File access validation performed for %s dataset".formatted(cloudPlatform),
        FlightTestUtils.getStepNames(flight),
        hasItems("ValidateBucketAccessStep"));
  }
}
