package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.model.DataSnapshotModel;
import bio.terra.model.DataSnapshotRequestModel;
import bio.terra.model.DataSnapshotSourceModel;
import bio.terra.model.DataSnapshotSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDataSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static bio.terra.pdao.PdaoConstant.PDAO_PREFIX;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DataSnapshotOperationTest {
    private static final boolean deleteOnTeardown = true;

    // private Logger logger = LoggerFactory.getLogger("bio.terra.controller.DataSnapshotOperationTest");

    // TODO: MORE TESTS to be done when we can ingest data:
    // - test more complex datasets with relationships
    // - test relationship walking with valid and invalid setups
    // TODO: MORE TESTS when we separate the value translation from the create
    // - test invalid row ids

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private BigQuery bigQuery;
    @Autowired private String bigQueryProjectId;
    @Autowired private JsonLoader jsonLoader;

    @MockBean
    private SamClientService samService;


    private List<String> createdDataSnapshotIds;
    private List<String> createdDatasetIds;
    private String dataSnapshotOriginalName;

    @Before
    public void setup() throws Exception {
        createdDataSnapshotIds = new ArrayList<>();
        createdDatasetIds = new ArrayList<>();
        ConnectedOperations.stubOutSamCalls(samService);
    }

    @After
    public void tearDown() throws Exception {
        if (deleteOnTeardown) {
            for (String dataSnapshotId : createdDataSnapshotIds) {
                deleteTestDataSnapshot(dataSnapshotId);
            }

            for (String datasetId : createdDatasetIds) {
                deleteTestDataset(datasetId);
            }
        }
    }

    @Test
    public void testHappyPath() throws Exception {
        DrDatasetSummaryModel datasetSummary = createTestDataset("datasnapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "datasnapshot-test-dataset-data.csv");

        DataSnapshotRequestModel dataSnapshotRequest = makeDataSnapshotTestRequest(
            datasetSummary, "datasnapshot-test.json");
        MockHttpServletResponse response = performCreateDataSnapshot(dataSnapshotRequest, "_happy_");
        DataSnapshotSummaryModel summaryModel = handleCreateDataSnapshotSuccessCase(dataSnapshotRequest, response);

        DataSnapshotModel dataSnapshotModel = getTestDataSnapshot(
            summaryModel.getId(), dataSnapshotRequest, datasetSummary);

        deleteTestDataSnapshot(dataSnapshotModel.getId());
        // Duplicate delete should work
        deleteTestDataSnapshot(dataSnapshotModel.getId());

        getNonexistentDataSnapshot(dataSnapshotModel.getId());
    }

    @Test
    public void testMinimal() throws Exception {
        DrDatasetSummaryModel datasetSummary = setupMinimalDataset();
        String datasetName = PDAO_PREFIX + datasetSummary.getName();
        long datasetParticipants = queryForCount(datasetName, "participant");
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample");
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        DataSnapshotRequestModel dataSnapshotRequest = makeDataSnapshotTestRequest(datasetSummary,
                "dataset-minimal-datasnapshot.json");
        MockHttpServletResponse response = performCreateDataSnapshot(dataSnapshotRequest, "");
        DataSnapshotSummaryModel summaryModel = handleCreateDataSnapshotSuccessCase(dataSnapshotRequest, response);
        getTestDataSnapshot(summaryModel.getId(), dataSnapshotRequest, datasetSummary);

        long dataSnapshotParticipants = queryForCount(summaryModel.getName(), "participant");
        assertThat("dataset participants loaded properly", dataSnapshotParticipants, equalTo(1L));
        long dataSnapshotSamples = queryForCount(summaryModel.getName(), "sample");
        assertThat("dataset samples loaded properly", dataSnapshotSamples, equalTo(2L));
    }

    @Test
    public void testArrayStruct() throws Exception {
        DrDatasetSummaryModel datasetSummary = setupArrayStructDataset();
        String datasetName = PDAO_PREFIX + datasetSummary.getName();
        long datasetParticipants = queryForCount(datasetName, "participant");
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample");
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        DataSnapshotRequestModel dataSnapshotRequest = makeDataSnapshotTestRequest(datasetSummary,
            "datasnapshot-test-array-struct.json");
        MockHttpServletResponse response = performCreateDataSnapshot(dataSnapshotRequest, "");
        DataSnapshotSummaryModel summaryModel = handleCreateDataSnapshotSuccessCase(dataSnapshotRequest, response);
        getTestDataSnapshot(summaryModel.getId(), dataSnapshotRequest, datasetSummary);

        long dataSnapshotParticipants = queryForCount(summaryModel.getName(), "participant");
        assertThat("dataset participants loaded properly", dataSnapshotParticipants, equalTo(2L));
        long dataSnapshotSamples = queryForCount(summaryModel.getName(), "sample");
        assertThat("dataset samples loaded properly", dataSnapshotSamples, equalTo(3L));

    }

    @Test
    public void testMinimalBadAsset() throws Exception {
        DrDatasetSummaryModel datasetSummary = setupMinimalDataset();
        DataSnapshotRequestModel dataSnapshotRequest = makeDataSnapshotTestRequest(datasetSummary,
                "dataset-minimal-datasnapshot-bad-asset.json");
        MockHttpServletResponse response = performCreateDataSnapshot(dataSnapshotRequest, "");
        ErrorModel errorModel = handleCreateDataSnapshotFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Asset"));
        assertThat(errorModel.getMessage(), containsString("NotARealAsset"));
    }

    @Test
    public void testEnumeration() throws Exception {
        DrDatasetSummaryModel datasetSummary = createTestDataset("datasnapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "datasnapshot-test-dataset-data.csv");
        DataSnapshotRequestModel dataSnapshotRequest =
            makeDataSnapshotTestRequest(datasetSummary, "datasnapshot-test.json");

        // Other unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same dataSnapshot summary that we made.
        List<DataSnapshotSummaryModel> dataSnapshotList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MockHttpServletResponse response = performCreateDataSnapshot(dataSnapshotRequest, "_enum_");
            DataSnapshotSummaryModel summaryModel = handleCreateDataSnapshotSuccessCase(dataSnapshotRequest, response);
            dataSnapshotList.add(summaryModel);
        }

        EnumerateDataSnapshotModel enumResponse = enumerateTestDataSnapshots();
        List<DataSnapshotSummaryModel> enumeratedArray = enumResponse.getItems();
        assertThat("total is correct", enumResponse.getTotal(), equalTo(5));

        // The enumeratedArray may contain more dataSnapshots than just the set we created,
        // but ours should be in order in the enumeration. So we do a merge waiting until we match
        // by id and then comparing contents.
        int compareIndex = 0;
        for (DataSnapshotSummaryModel anEnumeratedDataSnapshot : enumeratedArray) {
            if (anEnumeratedDataSnapshot.getId().equals(dataSnapshotList.get(compareIndex).getId())) {
                assertThat("MetadataEnumeration summary matches create summary",
                    anEnumeratedDataSnapshot, equalTo(dataSnapshotList.get(compareIndex)));
                compareIndex++;
            }
        }

        assertThat("we found all dataSnapshots", compareIndex, equalTo(5));

        for (int i = 0; i < 5; i++) {
            deleteTestDataSnapshot(enumeratedArray.get(i).getId());
        }
    }

    @Test
    public void testBadData() throws Exception {
        DrDatasetSummaryModel datasetSummary = createTestDataset("datasnapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "datasnapshot-test-dataset-data.csv");
        DataSnapshotRequestModel badDataRequest = makeDataSnapshotTestRequest(datasetSummary,
                "datasnapshot-test-baddata.json");

        MockHttpServletResponse response = performCreateDataSnapshot(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateDataSnapshotFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    // !!! This test is intended to be run manually when the BigQuery project gets orphans in it.
    // !!! It tries to delete all dataSnapshots from the project.
    // You have to comment out the @Ignore to run it and not forget to uncomment it when you are done.
    @Ignore
    @Test
    public void deleteAllBigQueryProjects() throws Exception {
        // Collect a list of dataSnapshots. Then delete each one.
        List<DatasetId> idList = new ArrayList<>();
        for (com.google.cloud.bigquery.Dataset dataset :  bigQuery.listDatasets().iterateAll()) {
            idList.add(dataset.getDatasetId());
        }

        for (DatasetId id : idList) {
            bigQuery.delete(id, BigQuery.DatasetDeleteOption.deleteContents());
        }
    }

    private DrDatasetSummaryModel setupMinimalDataset() throws Exception {
        DrDatasetSummaryModel datasetSummary = createTestDataset("dataset-minimal.json");
        loadCsvData(datasetSummary.getName(), "participant", "dataset-minimal-participant.csv");
        loadCsvData(datasetSummary.getName(), "sample", "dataset-minimal-sample.csv");
        return  datasetSummary;
    }

    private DrDatasetSummaryModel setupArrayStructDataset() throws Exception {
        DrDatasetSummaryModel datasetSum = createTestDataset("dataset-array-struct.json");
        loadJsonData(datasetSum.getName(), "participant", "dataset-array-struct-participant.json");
        loadJsonData(datasetSum.getName(), "sample", "dataset-array-struct-sample.json");
        return  datasetSum;
    }

    // create a dataset to create dataSnapshots in and return its id
    private DrDatasetSummaryModel createTestDataset(String resourcePath) throws Exception {
        DrDatasetRequestModel datasetRequest = jsonLoader.loadObject(resourcePath, DrDatasetRequestModel.class);
        datasetRequest.setName(Names.randomizeName(datasetRequest.getName()));

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(datasetRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DrDatasetSummaryModel datasetSummaryModel =
                objectMapper.readValue(response.getContentAsString(), DrDatasetSummaryModel.class);
        createdDatasetIds.add(datasetSummaryModel.getId());

        return datasetSummaryModel;
    }

    private void loadCsvData(String datasetName, String tableName, String resourcePath) throws Exception {
        FormatOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
        loadData(datasetName, tableName, resourcePath, csvOptions);
    }

    private void loadJsonData(String datasetName, String tableName, String resourcePath) throws Exception {
        loadData(datasetName, tableName, resourcePath, FormatOptions.json());
    }

    private void loadData(String datasetName,
                          String tableName,
                          String resourcePath,
                          FormatOptions options) throws Exception {
        String dataSnapshotName = PDAO_PREFIX + datasetName;
        String location = "US";
        TableId tableId = TableId.of(dataSnapshotName, tableName);

        WriteChannelConfiguration writeChannelConfiguration =
            WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(options).build();

        // The location must be specified; other fields can be auto-detected.
        JobId jobId = JobId.newBuilder().setLocation(location).build();
        TableDataWriteChannel writer = bigQuery.writer(jobId, writeChannelConfiguration);

        // Write data to writer
        try (OutputStream stream = Channels.newOutputStream(writer);
             InputStream csvStream = jsonLoader.getClassLoader().getResourceAsStream(resourcePath)) {
            IOUtils.copy(csvStream, stream);
        }

        // Get load job
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatus jobStatus = job.getStatus();
        List<BigQueryError> jobErrors = jobStatus.getExecutionErrors();
        if (jobErrors != null && jobErrors.size() != 0) {
            System.out.println("Errors loading dataset data: ");
            for (BigQueryError bqError : jobErrors) {
                System.out.println(bqError.toString());
            }
            fail("Failed to load dataset data");
        }
    }

    private DataSnapshotRequestModel makeDataSnapshotTestRequest(DrDatasetSummaryModel datasetSummaryModel,
                                                       String resourcePath) throws Exception {
        DataSnapshotRequestModel dataSnapshotRequest = jsonLoader.loadObject(
            resourcePath, DataSnapshotRequestModel.class);
        dataSnapshotRequest.getContents().get(0).getSource().setDatasetName(datasetSummaryModel.getName());
        return dataSnapshotRequest;
    }

    private MockHttpServletResponse performCreateDataSnapshot(
        DataSnapshotRequestModel dataSnapshotRequest, String infix) throws Exception {
        dataSnapshotOriginalName = dataSnapshotRequest.getName();
        String dataSnapshotName = Names.randomizeNameInfix(dataSnapshotOriginalName, infix);
        dataSnapshotRequest.setName(dataSnapshotName);

        String jsonRequest = objectMapper.writeValueAsString(dataSnapshotRequest);

        MvcResult result = mvc.perform(post("/api/repository/v1/datasnapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        MockHttpServletResponse response = validateJobModelAndWait(result);
        return response;
    }

    private DataSnapshotSummaryModel handleCreateDataSnapshotSuccessCase(DataSnapshotRequestModel dataSnapshotRequest,
                                                               MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        if (!responseStatus.is2xxSuccessful()) {
            String failMessage = "createTestDataSnapshot failed: status=" + responseStatus.toString();
            if (StringUtils.contains(responseBody, "message")) {
                // If the responseBody contains the word 'message', then we try to decode it as an ErrorModel
                // so we can generate good failure information.
                ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
                failMessage += " msg=" + errorModel.getMessage();
            } else {
                failMessage += " responseBody=" + responseBody;
            }
            fail(failMessage);
        }

        DataSnapshotSummaryModel summaryModel = objectMapper.readValue(responseBody, DataSnapshotSummaryModel.class);
        createdDataSnapshotIds.add(summaryModel.getId());

        assertThat(summaryModel.getDescription(), equalTo(dataSnapshotRequest.getDescription()));
        assertThat(summaryModel.getName(), equalTo(dataSnapshotRequest.getName()));

        // Reset the name in the dataSnapshot request
        dataSnapshotRequest.setName(dataSnapshotOriginalName);

        return summaryModel;
    }

    private ErrorModel handleCreateDataSnapshotFailureCase(MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        assertFalse("Expect create dataSnapshot failure", responseStatus.is2xxSuccessful());

        assertTrue("Error model was returned on failure",
                StringUtils.contains(responseBody, "message"));

        return objectMapper.readValue(responseBody, ErrorModel.class);
    }

    private void deleteTestDataSnapshot(String id) throws Exception {
        MvcResult result = mvc.perform(delete("/api/repository/v1/datasnapshots/" + id)).andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
        checkDeleteResponse(response);
    }

    private void deleteTestDataset(String id) throws Exception {
        // We only use this for @After, so we don't check return values
        MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + id)).andReturn();
        checkDeleteResponse(result.getResponse());
    }

    private void checkDeleteResponse(MockHttpServletResponse response) throws Exception {
        DeleteResponseModel responseModel =
            objectMapper.readValue(response.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    private EnumerateDataSnapshotModel enumerateTestDataSnapshots() throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasnapshots?offset=0&limit=100"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        EnumerateDataSnapshotModel summary =
                objectMapper.readValue(response.getContentAsString(), EnumerateDataSnapshotModel.class);
        return summary;
    }

    private DataSnapshotModel getTestDataSnapshot(String id,
                                        DataSnapshotRequestModel dataSnapshotRequest,
                                        DrDatasetSummaryModel datasetSummary) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasnapshots/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DataSnapshotModel dataSnapshotModel =
            objectMapper.readValue(response.getContentAsString(), DataSnapshotModel.class);

        assertThat(dataSnapshotModel.getDescription(), equalTo(dataSnapshotRequest.getDescription()));
        assertThat(dataSnapshotModel.getName(), startsWith(dataSnapshotRequest.getName()));

        assertThat("source array has one element",
                dataSnapshotModel.getSource().size(), equalTo(1));
        DataSnapshotSourceModel sourceModel = dataSnapshotModel.getSource().get(0);
        assertThat("dataSnapshot dataset summary is the same as from dataset",
                sourceModel.getDataset(), equalTo(datasetSummary));

        return dataSnapshotModel;
    }

    private void getNonexistentDataSnapshot(String id) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasnapshots/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        ErrorModel errorModel = objectMapper.readValue(response.getContentAsString(), ErrorModel.class);
        assertThat("proper not found error", errorModel.getMessage(), startsWith("DataSnapshot not found"));
    }

    // TODO: this can probably be common code for anything async
    private MockHttpServletResponse validateJobModelAndWait(MvcResult inResult) throws Exception {
        MvcResult result = inResult;
        while (true) {
            MockHttpServletResponse response = result.getResponse();
            HttpStatus status = HttpStatus.valueOf(response.getStatus());
            assertTrue("received expected jobs polling status",
                    (status == HttpStatus.ACCEPTED || status == HttpStatus.OK));

            JobModel jobModel = objectMapper.readValue(response.getContentAsString(), JobModel.class);
            String jobId = jobModel.getId();
            String locationUrl = response.getHeader("Location");
            assertNotNull("location URL was specified", locationUrl);

            switch (status) {
                case ACCEPTED:
                    // Not done case: sleep and probe using the header URL
                    assertThat("location header for probe", locationUrl,
                            equalTo(String.format("/api/repository/v1/jobs/%s", jobId)));

                    TimeUnit.SECONDS.sleep(1);
                    result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
                    break;

                case OK:
                    // Done case: get the result with the header URL and return the response;
                    // let the caller interpret the response
                    assertThat("location header for result", locationUrl,
                            equalTo(String.format("/api/repository/v1/jobs/%s/result", jobId)));
                    result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
                    return result.getResponse();

                default:
                    fail("invalid response status");
            }
        }
    }

    // Get the count of rows in a table or view
    private long queryForCount(String dataSnapshotName, String tableName) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
            .append(bigQueryProjectId).append('.').append(dataSnapshotName).append('.').append(tableName).append('`');
        String sql = builder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        return countValue.getLongValue();
    }

}
