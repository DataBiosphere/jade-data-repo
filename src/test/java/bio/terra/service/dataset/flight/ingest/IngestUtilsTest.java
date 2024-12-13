package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.ShortUUID;
import com.azure.storage.blob.BlobUrlParts;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestUtilsTest {
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final Dataset DATASET = new Dataset().id(DATASET_ID);
  @Mock private FlightContext context;
  @Mock private DatasetService datasetService;
  private FlightMap inputParameters;

  @BeforeEach
  void beforeEach() {
    inputParameters = new FlightMap();
  }

  private void initializeDatasetIdParameter() {
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    when(context.getInputParameters()).thenReturn(inputParameters);
  }

  @Test
  void getDatasetId() {
    initializeDatasetIdParameter();
    assertThat(IngestUtils.getDatasetId(context), equalTo(DATASET_ID));
  }

  @Test
  void getDatasetId_IllegalStateException() {
    when(context.getInputParameters()).thenReturn(inputParameters);
    assertThrows(IllegalStateException.class, () -> IngestUtils.getDatasetId(context));
  }

  @Test
  void getDataset() {
    initializeDatasetIdParameter();
    when(datasetService.retrieveForIngest(DATASET_ID)).thenReturn(DATASET);
    assertThat(IngestUtils.getDataset(context, datasetService), equalTo(DATASET));
  }

  @Test
  void getDataset_IllegalStateException() {
    when(context.getInputParameters()).thenReturn(inputParameters);
    assertThrows(
        IllegalStateException.class, () -> IngestUtils.getDataset(context, datasetService));
  }

  @ParameterizedTest
  @EnumSource(names = {"CSV", "ARRAY", "JSON"})
  void testJsonTypeIngest(FormatEnum format) {
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new IngestRequestModel().format(format));
    if (format == FormatEnum.CSV) {
      assertFalse(
          IngestUtils.isJsonTypeIngest(inputParameters),
          format + " ingest is not considered json-type");
    } else {
      assertTrue(
          IngestUtils.isJsonTypeIngest(inputParameters),
          format + " ingest is considered json-type");
    }
  }

  @Test
  void testParseValidBlobURL() {
    BlobUrlParts blobUrlParts =
        IngestUtils.validateBlobAzureBlobFileURL(
            "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/azure-simple-dataset-ingest-request.csv");
    assertThat("scheme is extracted", blobUrlParts.getScheme(), equalTo("https"));
    assertThat(
        "host is extracted",
        blobUrlParts.getHost(),
        equalTo("tdrconnectedsrc1.blob.core.windows.net"));
    assertThat(
        "container is extracted", blobUrlParts.getBlobContainerName(), equalTo("synapsetestdata"));
    assertThat(
        "Blob is extracted",
        blobUrlParts.getBlobName(),
        equalTo("test/azure-simple-dataset-ingest-request.csv"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/test/azure_simple_dataset_ingest.csv",
        "/test/----.json",
        "/test/nested/0_o.json"
      })
  void testValidURLWithSpecialCharacterBlobPaths(String urlSuffix) {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata" + urlSuffix);
  }

  @ParameterizedTest
  @MethodSource
  void testInvalidBlobUrl(String invalidFeature, String url) {
    assertThrows(
        InvalidBlobURLException.class,
        () -> IngestUtils.validateBlobAzureBlobFileURL(url),
        () -> "Blob URL with " + invalidFeature + " is invalid");
  }

  private static Stream<Arguments> testInvalidBlobUrl() {
    return Stream.of(
        arguments(
            "invalid scheme",
            "gs://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/azure-simple-dataset-ingest-request.csv"),
        arguments(
            "invalid host",
            "https://tdrconnectedsrc1/synapsetestdata/test/azure-simple-dataset-ingest-request.csv"),
        arguments(
            "invalid file extension",
            "https://tdrconnectedsrc1.blob.core.windows.net/test/azure-simple-dataset-ingest-request"),
        arguments(
            "illegal double dash",
            "https://tdrconnectedsrc1.blob.core.windows.net/synapsetest--data/test/azure-simple-dataset-ingest-request.csv"),
        arguments(
            "illegal uppercase",
            "https://tdrconnectedsrc1.blob.core.windows.net/SYNAPSETEST/test/azure-simple-dataset-ingest-request.csv"));
  }

  @Test
  void testShouldIgnoreUserSpecifiedRowIds() {
    // We should not find ourselves here: ingests default to append mode if unspecified.
    FlightMap flightMapNoUpdateStrategy = createFlightMap(null);
    assertFalse(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapNoUpdateStrategy),
        "Ingests with unspecified update strategy can specify their own row IDs");

    FlightMap flightMapAppend = createFlightMap(IngestRequestModel.UpdateStrategyEnum.APPEND);
    assertFalse(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapAppend),
        "Ingests in append mode can specify their own row IDs");

    FlightMap flightMapReplace = createFlightMap(IngestRequestModel.UpdateStrategyEnum.REPLACE);
    assertTrue(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapReplace),
        "Ingests in replace mode will have any specified row IDs unset");

    FlightMap flightMapMerge = createFlightMap(IngestRequestModel.UpdateStrategyEnum.MERGE);
    assertTrue(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapMerge),
        "Ingests in merge mode will have any specified row IDs unset");
  }

  @Test
  void testGetParquetFilePath() {
    String targetTableName = "sample";
    String flightId = "_" + ShortUUID.get();
    String expectedPath = "parquet/" + targetTableName + "/flight_" + flightId + ".parquet";
    assertEquals(IngestUtils.getParquetFilePath(targetTableName, flightId), expectedPath);
  }

  /**
   * @param updateStrategy to specify on a new stub ingest request
   * @return a new FlightMap whose ingest request contains the provided update strategy
   */
  private FlightMap createFlightMap(IngestRequestModel.UpdateStrategyEnum updateStrategy) {
    IngestRequestModel ingestRequest = new IngestRequestModel().updateStrategy(updateStrategy);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), ingestRequest);
    return inputParameters;
  }
}
