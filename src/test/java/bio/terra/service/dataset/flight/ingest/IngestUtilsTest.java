package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
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

  public void testValidURLWithSpecialCharaterBlobPaths() {
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/azure_simple_dataset_ingest_request.csv");
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/AZURE_SIMPLE_DATASET_INGEST_REQUEST.CSV");
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/----.json");
    IngestUtils.validateBlobAzureBlobFileURL(
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/nested/0_o.json");
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
}
