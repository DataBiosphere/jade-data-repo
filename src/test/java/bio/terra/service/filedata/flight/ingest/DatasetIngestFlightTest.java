package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightTestUtils.mockFlightAppConfigSetup;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import java.util.ArrayList;
import java.util.List;
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
@Tag(Unit.TAG)
class DatasetIngestFlightTest {
  @Mock private ApplicationContext context;
  @Mock private DatasetSummary datasetSummary;
  @Mock private ConfigurationService configurationService;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    DatasetService datasetService = mock(DatasetService.class);
    when(datasetService.retrieve(DATASET_ID)).thenReturn(new Dataset(datasetSummary));

    mockFlightAppConfigSetup(context);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);
    when(context.getBean(ConfigurationService.class)).thenReturn(configurationService);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
  }

  void initJsonTypeIngestMocks() {
    when(datasetSummary.getId()).thenReturn(DATASET_ID);
    when(configurationService.getParameterValue(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS)).thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.LOAD_HISTORY_WAIT_SECONDS))
        .thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE))
        .thenReturn(1);
  }

  @ParameterizedTest
  @MethodSource
  void testDatasetIngestValidationSteps(
      CloudPlatform cloudPlatform, FormatEnum format, boolean bulkMode) {
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(cloudPlatform);

    var request =
        new IngestRequestModel().format(format).bulkMode(bulkMode).profileId(UUID.randomUUID());
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    boolean jsonTypeIngest = IngestUtils.isJsonTypeIngest(inputParameters);
    if (jsonTypeIngest) {
      initJsonTypeIngestMocks();
    }

    var flight = new DatasetIngestFlight(inputParameters, context);
    List<String> stepNames = FlightTestUtils.getStepNames(flight);
    String flightDescription =
        "Combined %s ingest to %s dataset (bulk mode = %b)"
            .formatted(format, cloudPlatform, bulkMode);

    assertThat(
        flightDescription + " locks dataset, validates file accessibility, then unlocks dataset",
        stepNames,
        containsInRelativeOrder(
            "LockDatasetStep",
            "ValidateBucketAccessStep", // Validates accessibility of directly specified files
            "UnlockDatasetStep"));

    LockDatasetStep lockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, LockDatasetStep.class);
    if (bulkMode) {
      assertFalse(
          lockDatasetStep.isSharedLock(), flightDescription + " obtains exclusive dataset lock");
    } else {
      assertTrue(
          lockDatasetStep.isSharedLock(), flightDescription + " obtains shared dataset lock");
    }

    assertFalse(
        lockDatasetStep.shouldSuppressNotFoundException(),
        flightDescription + " does not suppress 'dataset not found' exceptions");

    UnlockDatasetStep unlockDatasetStep =
        FlightTestUtils.getStepWithClass(flight, UnlockDatasetStep.class);
    if (bulkMode) {
      assertFalse(
          unlockDatasetStep.isSharedLock(), flightDescription + " removes exclusive dataset lock");
    } else {
      assertTrue(
          unlockDatasetStep.isSharedLock(), flightDescription + " removes shared dataset lock");
    }

    if (jsonTypeIngest) {
      // CSV ingests can only be run for metadata, not files
      String expectedIndirectFileValidationStep =
          CloudPlatformWrapper.of(cloudPlatform).isGcp()
              ? "IngestJsonFileSetupGcpStep"
              : "IngestJsonFileSetupAzureStep";
      assertThat(
          flightDescription + " validates accessibility of indirectly specified files",
          stepNames,
          hasItems(expectedIndirectFileValidationStep));
    }

    if (CloudPlatformWrapper.of(cloudPlatform).isAzure()) {
      assertThat(
          flightDescription
              + " validates tabular data and file references from scratch table before publishing",
          stepNames,
          containsInRelativeOrder(
              "IngestCreateIngestRequestDataSourceStep",
              "IngestCreateTargetDataSourceStep",
              "IngestCreateScratchParquetFilesStep",
              "IngestValidateScratchTableStep",
              "IngestValidateScratchTableFilerefsStep",
              "IngestCreateParquetFilesStep",
              "IngestCleanAzureStep"));
    }
  }

  private static Stream<Arguments> testDatasetIngestValidationSteps() {
    List<Arguments> arguments = new ArrayList<>();
    for (var platform : CloudPlatform.values()) {
      for (var format : FormatEnum.values()) {
        for (var bulkMode : List.of(false, true)) {
          arguments.add(arguments(platform, format, bulkMode));
        }
      }
    }
    return arguments.stream();
  }
}
