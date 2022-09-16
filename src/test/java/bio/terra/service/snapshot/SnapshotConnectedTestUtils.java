package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import bio.terra.common.TestUtils;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.stringtemplate.v4.ST;

public class SnapshotConnectedTestUtils {

  static SnapshotRequestModel makeSnapshotTestRequest(
      JsonLoader jsonLoader,
      DatasetSummaryModel datasetSummaryModel,
      String resourcePath,
      UUID profileId)
      throws Exception {
    SnapshotRequestModel snapshotRequest =
        jsonLoader.loadObject(resourcePath, SnapshotRequestModel.class);
    SnapshotRequestContentsModel content = snapshotRequest.getContents().get(0);
    // TODO SingleDatasetSnapshot
    String newDatasetName = datasetSummaryModel.getName();
    String origDatasetName = content.getDatasetName();
    // swap in the correct dataset name (with the id at the end)
    content.setDatasetName(newDatasetName);
    // provide the profileId for the request.  The API does not require this value.
    snapshotRequest.profileId(profileId);
    if (content.getMode().equals(SnapshotRequestContentsModel.ModeEnum.BYQUERY)) {
      // if its by query, also set swap in the correct dataset name in the query
      String query = content.getQuerySpec().getQuery();
      content.getQuerySpec().setQuery(query.replace(origDatasetName, newDatasetName));
    }
    return snapshotRequest;
  }

  static SnapshotModel getTestSnapshot(
      MockMvc mvc,
      ObjectMapper objectMapper,
      UUID id,
      SnapshotRequestModel snapshotRequest,
      DatasetSummaryModel datasetSummary)
      throws Exception {
    MvcResult result =
        mvc.perform(get("/api/repository/v1/snapshots/" + id))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    SnapshotModel snapshotModel =
        objectMapper.readValue(response.getContentAsString(), SnapshotModel.class);

    assertThat(snapshotModel.getDescription(), equalTo(snapshotRequest.getDescription()));
    assertThat(snapshotModel.getName(), startsWith(snapshotRequest.getName()));

    assertThat("source array has one element", snapshotModel.getSource().size(), equalTo(1));
    SnapshotSourceModel sourceModel = snapshotModel.getSource().get(0);

    assertThat(
        "snapshot dataset summary is the same as from dataset",
        sourceModel.getDataset(),
        equalTo(datasetSummary));

    return snapshotModel;
  }

  static MvcResult launchCreateSnapshot(
      MockMvc mvc, SnapshotRequestModel snapshotRequest, String infix) throws Exception {
    if (infix != null) {
      String snapshotName = Names.randomizeNameInfix(snapshotRequest.getName(), infix);
      snapshotRequest.setName(snapshotName);
    }

    String jsonRequest = TestUtils.mapToJson(snapshotRequest);

    return mvc.perform(
            post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
        // TODO: swagger field validation errors do not set content type; they log and return
        // nothing
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andReturn();
  }

  static MockHttpServletResponse performCreateSnapshot(
      ConnectedOperations connectedOperations,
      MockMvc mvc,
      SnapshotRequestModel snapshotRequest,
      String infix)
      throws Exception {
    MvcResult result = SnapshotConnectedTestUtils.launchCreateSnapshot(mvc, snapshotRequest, infix);
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
    return response;
  }

  static SnapshotSummaryModel validateSnapshotCreated(
      ConnectedOperations connectedOperations,
      SnapshotRequestModel snapshotRequest,
      MockHttpServletResponse response)
      throws Exception {
    SnapshotSummaryModel summaryModel =
        connectedOperations.handleCreateSnapshotSuccessCase(response);

    assertThat(summaryModel.getDescription(), equalTo(snapshotRequest.getDescription()));
    assertThat(summaryModel.getName(), equalTo(snapshotRequest.getName()));

    return summaryModel;
  }

  // create a dataset to create snapshots in and return its id
  static DatasetSummaryModel createTestDataset(
      ConnectedOperations connectedOperations,
      BillingProfileModel billingProfile,
      String resourcePath)
      throws Exception {
    return connectedOperations.createDataset(billingProfile, resourcePath);
  }

  static SnapshotPreviewModel getTablePreview(
      ConnectedOperations connectedOperations,
      UUID snapshotId,
      String tableName,
      int limit,
      int offset,
      String filter)
      throws Exception {
    return connectedOperations.retrieveSnapshotPreviewByIdSuccess(
        snapshotId, tableName, limit, offset, filter);
  }

  static ErrorModel getTablePreviewFailure(
      ConnectedOperations connectedOperations,
      UUID snapshotId,
      String tableName,
      int limit,
      int offset,
      String filter,
      HttpStatus expectedStatus)
      throws Exception {
    return connectedOperations.retrieveSnapshotPreviewByIdFailure(
        snapshotId, tableName, limit, offset, filter, expectedStatus);
  }

  static void loadCsvData(
      ConnectedOperations connectedOperations,
      JsonLoader jsonLoader,
      Storage storage,
      String bucket,
      UUID datasetId,
      String tableName,
      String resourcePath)
      throws Exception {
    loadData(
        connectedOperations,
        jsonLoader,
        storage,
        bucket,
        datasetId,
        tableName,
        resourcePath,
        IngestRequestModel.FormatEnum.CSV);
  }

  static void loadData(
      ConnectedOperations connectedOperations,
      JsonLoader jsonLoader,
      Storage storage,
      String bucket,
      UUID datasetId,
      String tableName,
      String resourcePath,
      IngestRequestModel.FormatEnum format)
      throws Exception {

    BlobInfo stagingBlob =
        BlobInfo.newBuilder(bucket, UUID.randomUUID() + "-" + resourcePath).build();
    byte[] data = IOUtils.toByteArray(jsonLoader.getClassLoader().getResource(resourcePath));

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(format)
            .path("gs://" + stagingBlob.getBucket() + "/" + stagingBlob.getName());

    if (format.equals(IngestRequestModel.FormatEnum.CSV)) {
      ingestRequest.csvSkipLeadingRows(1);
      ingestRequest.csvGenerateRowIds(false);
    }

    try {
      storage.create(stagingBlob, data);
      connectedOperations.ingestTableSuccess(datasetId, ingestRequest);
    } finally {
      storage.delete(stagingBlob.getBlobId());
    }
  }

  private static final String queryForCountTemplate =
      "SELECT COUNT(*) FROM `<project>.<snapshot>.<table>`";

  // Get the count of rows in a table or view
  static long queryForCount(String snapshotName, String tableName, BigQueryProject bigQueryProject)
      throws Exception {
    String bigQueryProjectId = bigQueryProject.getProjectId();
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    ST sqlTemplate = new ST(queryForCountTemplate);
    sqlTemplate.add("project", bigQueryProjectId);
    sqlTemplate.add("snapshot", snapshotName);
    sqlTemplate.add("table", tableName);

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
    TableResult result = bigQuery.query(queryConfig);
    FieldValueList row = result.iterateAll().iterator().next();
    FieldValue countValue = row.get(0);
    return countValue.getLongValue();
  }
}
