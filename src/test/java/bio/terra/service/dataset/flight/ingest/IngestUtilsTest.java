package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.category.Unit;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobUrlParts;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IngestUtilsTest {

  @Mock ConfigurationService configurationService;

  @Test
  public void testParseValidBlobURL() {
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

  public void testValidURLWithSpecialCharacterBlobPaths() {
    String urlPrefix = "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata";
    IngestUtils.validateBlobAzureBlobFileURL(urlPrefix + "/test/azure_simple_dataset_ingest.csv");
    IngestUtils.validateBlobAzureBlobFileURL(urlPrefix + "/test/AZURE_SIMPLE_DATASET_INGEST.CSV");
    IngestUtils.validateBlobAzureBlobFileURL(urlPrefix + "/test/----.json");
    IngestUtils.validateBlobAzureBlobFileURL(urlPrefix + "/test/nested/0_o.json");
  }

  @Test(expected = InvalidBlobURLException.class)
  public void testInvalidScheme() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "gs://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/azure-simple-dataset-ingest-request.csv");
  }

  @Test(expected = InvalidBlobURLException.class)
  public void testInvalidHost() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1/synapsetestdata/test/azure-simple-dataset-ingest-request.csv");
  }

  @Test(expected = InvalidBlobURLException.class)
  public void testInvalidFileExtension() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/test/azure-simple-dataset-ingest-request");
  }

  @Test(expected = InvalidBlobURLException.class)
  public void testNoDoubleDash() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetest--data/test/azure-simple-dataset-ingest-request.csv");
  }

  @Test(expected = InvalidBlobURLException.class)
  public void testNoUpperCase() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/SYNAPSETEST/test/azure-simple-dataset-ingest-request.csv");
  }

  @Test
  public void testShouldIgnoreUserSpecifiedRowIds() {
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
