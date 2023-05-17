package bio.terra.service.filedata.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.FlightTestUtils;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class FileIngestBulkFlightTest {
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
    inputParameters.put(LoadMapKeys.DRIVER_WAIT_SECONDS, 1);
    inputParameters.put(LoadMapKeys.LOAD_HISTORY_WAIT_SECONDS, 1);
    inputParameters.put(LoadMapKeys.LOAD_HISTORY_COPY_CHUNK_SIZE, 1);
  }

  @ParameterizedTest
  @MethodSource
  void testFileIngestBulkArrayFlightValidatesFileAccess(
      CloudPlatform cloudPlatform, boolean bulkMode) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var request = new BulkLoadArrayRequestModel().bulkMode(bulkMode);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);
    inputParameters.put(LoadMapKeys.IS_ARRAY, true);

    var flight = new FileIngestBulkFlight(inputParameters, context);
    assertThat(
        "Bulk array file access validation performed for %s dataset (bulk mode = %b)"
            .formatted(cloudPlatform, bulkMode),
        FlightTestUtils.getStepNames(flight),
        hasItems("ValidateBucketAccessStep"));
  }

  private static Stream<Arguments> testFileIngestBulkArrayFlightValidatesFileAccess() {
    return Stream.of(
        arguments(CloudPlatform.GCP, false),
        arguments(CloudPlatform.GCP, true),
        arguments(CloudPlatform.AZURE, false),
        arguments(CloudPlatform.AZURE, true));
  }

  @ParameterizedTest
  @MethodSource
  void testFileIngestBulkJsonFlightValidatesFileAccess(
      CloudPlatform cloudPlatform, boolean bulkMode, String expectedIndirectFileValidationStep) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var request = new BulkLoadRequestModel().bulkMode(bulkMode);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);
    inputParameters.put(LoadMapKeys.IS_ARRAY, false);

    var flight = new FileIngestBulkFlight(inputParameters, context);
    assertThat(
        "Bulk JSON file access validation performed for %s dataset (bulk mode = %b)"
            .formatted(cloudPlatform, bulkMode),
        FlightTestUtils.getStepNames(flight),
        hasItems("ValidateBucketAccessStep", expectedIndirectFileValidationStep));
  }

  private static Stream<Arguments> testFileIngestBulkJsonFlightValidatesFileAccess() {
    return Stream.of(
        arguments(CloudPlatform.GCP, false, "IngestPopulateFileStateFromFileGcpStep"),
        arguments(CloudPlatform.GCP, true, "IngestBulkGcpBulkFileStep"),
        arguments(CloudPlatform.AZURE, false, "IngestPopulateFileStateFromFileAzureStep"),
        arguments(CloudPlatform.AZURE, true, "IngestBulkGcpBulkFileStep"));
  }
}
