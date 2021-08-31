package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.dataset.exception.InvalidIngestStrategyException;
import bio.terra.service.dataset.exception.InvalidUriException;
import com.azure.storage.blob.BlobUrlParts;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class IngestUtilsTest {

  @Mock ConfigurationService configurationService;

  @Test
  public void testParseValidSingleFile() {
    IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some/file.json");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getPath(), equalTo("some/file.json"));
    assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(false));
  }

  @Test
  public void testParseValidPatternAtEnd() {
    IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some/prefix*");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getPath(), equalTo("some/prefix*"));
    assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
  }

  @Test
  public void testParseValidPatternInMiddle() {
    IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some*pattern");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getPath(), equalTo("some*pattern"));
    assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
  }

  @Test(expected = InvalidUriException.class)
  public void testNotAGsUri() {
    IngestUtils.parseBlobUri("https://foo.com/bar");
  }

  @Test(expected = InvalidUriException.class)
  public void testInvalidBucketWildcard() {
    IngestUtils.parseBlobUri("gs://some-bucket-*/some/file/path");
  }

  @Test(expected = InvalidUriException.class)
  public void testInvalidMultiWildcard() {
    IngestUtils.parseBlobUri("gs://some-bucket/some/prefix*/some*pattern");
  }

  // ------- Azure Blob URL validation-----------

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
        "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata/test/azure-simple--dataset-ingest-request.csv");
  }

  @Test(expected = InvalidIngestStrategyException.class)
  public void testMaxLineIngestCheck() {
    long numLines = 11;
    when(configurationService.getParameterValue(ConfigEnum.LOAD_BULK_FILES_MAX)).thenReturn(10);
    IngestUtils.checkForLargeIngestRequests(
        numLines, configurationService.getParameterValue(ConfigEnum.LOAD_BULK_FILES_MAX));
  }

  @Test
  public void testSmallLineIngestCheck() {
    long numLines = 9;
    when(configurationService.getParameterValue(ConfigEnum.LOAD_BULK_FILES_MAX)).thenReturn(10);
    IngestUtils.checkForLargeIngestRequests(
        numLines, configurationService.getParameterValue(ConfigEnum.LOAD_BULK_FILES_MAX));
  }
}
