package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobUrlParts;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("bio.terra.common.category.Unit")
public class IngestUtilsTest {

  @ParameterizedTest
  @EnumSource(names = {"CSV", "ARRAY", "JSON"})
  void testJsonTypeIngest(FormatEnum format) {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new IngestRequestModel().format(format));
    if (format == FormatEnum.CSV) {
      assertFalse(
          format + " ingest is not considered json-type",
          IngestUtils.isJsonTypeIngest(inputParameters));
    } else {
      assertTrue(
          format + " ingest is considered json-type",
          IngestUtils.isJsonTypeIngest(inputParameters));
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
        "Blob URL with " + invalidFeature + " is invalid",
        InvalidBlobURLException.class,
        () -> IngestUtils.validateBlobAzureBlobFileURL(url));
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
        "Ingests with unspecified update strategy can specify their own row IDs",
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapNoUpdateStrategy));

    FlightMap flightMapAppend = createFlightMap(IngestRequestModel.UpdateStrategyEnum.APPEND);
    assertFalse(
        "Ingests in append mode can specify their own row IDs",
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapAppend));

    FlightMap flightMapReplace = createFlightMap(IngestRequestModel.UpdateStrategyEnum.REPLACE);
    assertTrue(
        "Ingests in replace mode will have any specified row IDs unset",
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapReplace));

    FlightMap flightMapMerge = createFlightMap(IngestRequestModel.UpdateStrategyEnum.MERGE);
    assertTrue(
        "Ingests in merge mode will have any specified row IDs unset",
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapMerge));
  }

  /**
   * @param updateStrategy to specify on a new stub ingest request
   * @return a new FlightMap whose ingest request contains the provided update strategy
   */
  private FlightMap createFlightMap(IngestRequestModel.UpdateStrategyEnum updateStrategy) {
    FlightMap inputParameters = new FlightMap();
    IngestRequestModel ingestRequest = new IngestRequestModel().updateStrategy(updateStrategy);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), ingestRequest);
    return inputParameters;
  }
}
