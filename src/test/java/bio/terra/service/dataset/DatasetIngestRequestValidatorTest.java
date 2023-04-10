package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetIngestRequestValidatorTest {

  @Autowired private MockMvc mvc;
  @MockBean private DatasetService datasetService;

  private MvcResult postRequest(String url, String content) throws Exception {
    return mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(content))
        .andExpect(status().is4xxClientError())
        .andReturn();
  }

  private ErrorModel expectBadPostRequest(String url, String content) throws Exception {
    MvcResult result = postRequest(url, content);
    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));
    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  @Test
  public void testAzureIngestRequestParameters() throws Exception {
    Dataset dataset = mock(Dataset.class);
    DatasetSummary datasetSummary = mock(DatasetSummary.class);
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(CloudPlatform.AZURE);
    when(dataset.getDatasetSummary()).thenReturn(datasetSummary);
    when(datasetService.retrieve(any())).thenReturn(dataset);

    var nullIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(null)
            .csvFieldDelimiter(null)
            .csvQuote(null);
    String ingest_url = String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID());
    ErrorModel nullErrorModel = expectBadPostRequest(ingest_url, TestUtils.mapToJson(nullIngest));
    assertThat(
        "Validation catches all null parameters", nullErrorModel.getErrorDetail(), hasSize(3));
    for (String error : nullErrorModel.getErrorDetail()) {
      assertThat("Validation catches null parameters", error, containsString("defined"));
    }

    var invalidIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(-1)
            .csvFieldDelimiter("toolong")
            .csvQuote("toolong");
    ErrorModel invalidErrorModel =
        expectBadPostRequest(ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(3));
    var csvSkipLeadingRowsError = invalidErrorModel.getErrorDetail().get(0);
    var csvFieldDelimiterError = invalidErrorModel.getErrorDetail().get(1);
    var csvQuoteError = invalidErrorModel.getErrorDetail().get(2);
    assertThat(
        "Validator catches invalid 'csvSkipLeadingRows'",
        csvSkipLeadingRowsError,
        containsString("'csvSkipLeadingRows' must be a positive integer, was '-1."));
    assertThat(
        "Validator catches invalid 'csvFieldDelimiter'",
        csvFieldDelimiterError,
        containsString("'csvFieldDelimiter' must be a single character, was 'toolong'."));
    assertThat(
        "Validator catches invalid 'csvQuote'",
        csvQuoteError,
        containsString("'csvQuote' must be a single character, was 'toolong'."));
  }

  @Test
  public void testInvalidIngestByArray() throws Exception {
    var invalidIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.ARRAY);
    String ingest_url = String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID());
    ErrorModel invalidErrorModel =
        expectBadPostRequest(ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(2));
    var pathIsPresentError = invalidErrorModel.getErrorDetail().get(0);
    var payloadIsMissingError = invalidErrorModel.getErrorDetail().get(1);
    assertThat(
        "Validator catches invalid 'path' and 'format' combo",
        pathIsPresentError,
        containsString("Path should not be specified when ingesting from an array"));
    assertThat(
        "Validator catches invalid 'format' and 'records' combo",
        payloadIsMissingError,
        containsString("Records is required when ingesting as an array"));
  }

  @Test
  public void testInvalidIngestByPath() throws Exception {
    var invalidIngest =
        new IngestRequestModel()
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.JSON)
            .addRecordsItem(Map.of("foo", "bar"));
    String ingest_url = String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID());
    ErrorModel invalidErrorModel =
        expectBadPostRequest(ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(2));
    var pathIsMissingError = invalidErrorModel.getErrorDetail().get(0);
    var payloadIsPresentError = invalidErrorModel.getErrorDetail().get(1);
    assertThat(
        "Validator catches invalid 'path' and 'format' combo",
        pathIsMissingError,
        containsString("Path is required when ingesting from a cloud object"));
    assertThat(
        "Validator catches invalid 'records' and 'format' combo",
        payloadIsPresentError,
        containsString("Records should not be specified when ingesting from a path"));
  }

  @Test
  public void testInvalidIngestWithNullIntFields() throws Exception {
    var invalidIngest =
        new IngestRequestModel()
            .table("myTable")
            .path("gs://foo/bar.json")
            .format(IngestRequestModel.FormatEnum.JSON)
            .maxBadRecords(null)
            .maxFailedFileLoads(null);
    String ingest_url = String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID());
    ErrorModel invalidErrorModel =
        expectBadPostRequest(ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(2));
    for (String error : invalidErrorModel.getErrorDetail()) {
      assertThat("Validation catches null parameters", error, containsString("NotNull"));
    }
  }

  @Test
  public void testInvalidBulkIngestWithNullIntFields() throws Exception {
    var invalidIngest =
        new BulkLoadRequestModel()
            .loadControlFile("gs://foo/bar.json")
            .loadTag("foo")
            .profileId(UUID.randomUUID())
            .maxFailedFileLoads(null);
    String bulk_ingest_url =
        String.format("/api/repository/v1/datasets/%s/files/bulk", UUID.randomUUID());
    ErrorModel invalidErrorModel =
        expectBadPostRequest(bulk_ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(1));
    var maxFailedFileLoadsError = invalidErrorModel.getErrorDetail().get(0);
    assertThat(
        "Validator catches null 'maxFailedFileLoadsError'",
        maxFailedFileLoadsError,
        containsString("maxFailedFileLoads: 'NotNull'"));
  }

  @Test
  public void testInvalidBulkArrayIngestWithNullIntFields() throws Exception {
    var invalidIngest =
        new BulkLoadArrayRequestModel()
            .loadArray(
                List.of(
                    new BulkLoadFileModel()
                        .sourcePath("gs://foo/source.txt")
                        .targetPath("/foo/bar")))
            .profileId(UUID.randomUUID())
            .loadTag("foo")
            .maxFailedFileLoads(null);
    String bulk_ingest_url =
        String.format("/api/repository/v1/datasets/%s/files/bulk/array", UUID.randomUUID());
    ErrorModel invalidErrorModel =
        expectBadPostRequest(bulk_ingest_url, TestUtils.mapToJson(invalidIngest));
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(1));
    var maxFailedFileLoadsError = invalidErrorModel.getErrorDetail().get(0);
    assertThat(
        "Validator catches null 'maxFailedFileLoadsError'",
        maxFailedFileLoadsError,
        containsString("maxFailedFileLoads: 'NotNull'"));
  }

  @Test
  public void testBulkIngestRequiresLoadTag() throws Exception {
    var invalidIngest =
        new BulkLoadRequestModel()
            .profileId(UUID.randomUUID())
            .loadControlFile("gs://foo/bar.json")
            .loadTag("")
            .bulkMode(true);
    String url = String.format("/api/repository/v1/datasets/%s/files/bulk", UUID.randomUUID());
    var invalidResult = postRequest(url, TestUtils.mapToJson(invalidIngest));
    Exception ex = invalidResult.getResolvedException();
    assertNotNull(ex);
    assertEquals("Load tag is required for isBulkMode", ex.getMessage());
  }

  @Test
  public void testBulkArrayIngestRequiresLoadTag() throws Exception {
    var invalidIngest =
        new BulkLoadArrayRequestModel()
            .profileId(UUID.randomUUID())
            .loadTag("")
            .bulkMode(true)
            .loadArray(
                List.of(
                    new BulkLoadFileModel()
                        .sourcePath("gs://foo/source.txt")
                        .targetPath("/foo/bar")));
    String url =
        String.format("/api/repository/v1/datasets/%s/files/bulk/array", UUID.randomUUID());
    var invalidResult = postRequest(url, TestUtils.mapToJson(invalidIngest));
    Exception ex = invalidResult.getResolvedException();
    assertNotNull(ex);
    assertEquals("Load tag is required for isBulkMode", ex.getMessage());
  }
}
