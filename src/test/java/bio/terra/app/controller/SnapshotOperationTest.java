package bio.terra.app.controller;

import bio.terra.category.Connected;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.resourcemanagement.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.resourcemanagement.DataLocationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
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
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.junit.After;
import org.junit.Before;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
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
public class SnapshotOperationTest {
    private static final boolean deleteOnTeardown = true;

    // private Logger logger = LoggerFactory.getLogger("bio.terra.app.controller.SnapshotOperationTest");

    // TODO: MORE TESTS to be done when we can ingest data:
    // - test more complex datasets with relationships
    // - test relationship walking with valid and invalid setups
    // TODO: MORE TESTS when we separate the value translation from the create
    // - test invalid row ids

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private DatasetDao datasetDao;
    @Autowired private ProfileDao profileDao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samService;

    private List<String> createdSnapshotIds;
    private List<String> createdDatasetIds;
    private String snapshotOriginalName;
    private BillingProfile billingProfile;

    @Before
    public void setup() throws Exception {
        // TODO all of this should be refactored to use connected operations, and that should be made a component
        createdSnapshotIds = new ArrayList<>();
        createdDatasetIds = new ArrayList<>();
        connectedOperations.stubOutSamCalls(samService);
        billingProfile = ProfileFixtures.billingProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
    }

    @After
    public void tearDown() throws Exception {
        if (deleteOnTeardown) {
            for (String snapshotId : createdSnapshotIds) {
                deleteTestSnapshot(snapshotId);
            }

            for (String datasetId : createdDatasetIds) {
                deleteTestDataset(datasetId);
            }
            profileDao.deleteBillingProfileById(billingProfile.getId());
        }
    }

    @Test
    public void testHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = handleCreateSnapshotSuccessCase(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        deleteTestSnapshot(snapshotModel.getId());

        getNonexistentSnapshot(snapshotModel.getId());
    }

    @Test
    public void testMinimal() throws Exception {
        DatasetSummaryModel datasetSummary = setupMinimalDataset();
        String datasetName = PDAO_PREFIX + datasetSummary.getName();
        BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetSummary.getName());
        long datasetParticipants = queryForCount(datasetName, "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary,
                "dataset-minimal-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "");
        SnapshotSummaryModel summaryModel = handleCreateSnapshotSuccessCase(snapshotRequest, response);
        getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        long snapshotParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(1L));
        long snapshotSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", snapshotSamples, equalTo(2L));
    }

    @Test
    public void testArrayStruct() throws Exception {
        DatasetSummaryModel datasetSummary = setupArrayStructDataset();
        String datasetName = PDAO_PREFIX + datasetSummary.getName();
        BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetSummary.getName());
        long datasetParticipants = queryForCount(datasetName, "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary,
            "snapshot-array-struct.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "");
        SnapshotSummaryModel summaryModel = handleCreateSnapshotSuccessCase(snapshotRequest, response);
        getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        long snapshotParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(2L));
        long snapshotSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", snapshotSamples, equalTo(3L));
    }

    @Test
    public void testMinimalBadAsset() throws Exception {
        DatasetSummaryModel datasetSummary = setupMinimalDataset();
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary,
                "dataset-minimal-snapshot-bad-asset.json");
        MvcResult result = launchCreateSnapshot(snapshotRequest, "");
        ErrorModel errorModel = handleCreateSnapshotFailureCase(result.getResponse());
        assertThat(errorModel.getMessage(), containsString("Asset"));
        assertThat(errorModel.getMessage(), containsString("NotARealAsset"));
    }

    @Test
    public void testEnumeration() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "snapshot-test-dataset-data.csv");
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");

        // Other unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same snapshot summary that we made.
        List<SnapshotSummaryModel> snapshotList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_en_");
            SnapshotSummaryModel summaryModel = handleCreateSnapshotSuccessCase(snapshotRequest, response);
            snapshotList.add(summaryModel);
        }

        when(samService.listAuthorizedResources(any(), any())).thenReturn(snapshotList.stream().map(snapshot ->
            new ResourceAndAccessPolicy().resourceId(snapshot.getId())).collect(Collectors.toList()));
        EnumerateSnapshotModel enumResponse = enumerateTestSnapshots();
        List<SnapshotSummaryModel> enumeratedArray = enumResponse.getItems();
        assertThat("total is correct", enumResponse.getTotal(), equalTo(5));

        // The enumeratedArray may contain more snapshots than just the set we created,
        // but ours should be in order in the enumeration. So we do a merge waiting until we match
        // by id and then comparing contents.
        int compareIndex = 0;
        for (SnapshotSummaryModel anEnumeratedSnapshot : enumeratedArray) {
            if (anEnumeratedSnapshot.getId().equals(snapshotList.get(compareIndex).getId())) {
                assertThat("MetadataEnumeration summary matches create summary",
                    anEnumeratedSnapshot, equalTo(snapshotList.get(compareIndex)));
                compareIndex++;
            }
        }

        assertThat("we found all snapshots", compareIndex, equalTo(5));

        for (int i = 0; i < 5; i++) {
            deleteTestSnapshot(enumeratedArray.get(i).getId());
        }
    }

    @Test
    public void testBadData() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "snapshot-test-dataset-data.csv");
        SnapshotRequestModel badDataRequest = makeSnapshotTestRequest(datasetSummary,
                "snapshot-test-snapshot-baddata.json");

        MockHttpServletResponse response = performCreateSnapshot(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    private DatasetSummaryModel setupMinimalDataset() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("dataset-minimal.json");
        loadCsvData(datasetSummary.getName(), "participant", "dataset-minimal-participant.csv");
        loadCsvData(datasetSummary.getName(), "sample", "dataset-minimal-sample.csv");
        return  datasetSummary;
    }

    private DatasetSummaryModel setupArrayStructDataset() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("dataset-array-struct.json");
        loadJsonData(datasetSummary.getName(), "participant", "dataset-array-struct-participant.json");
        loadJsonData(datasetSummary.getName(), "sample", "dataset-array-struct-sample.json");
        return  datasetSummary;
    }

    // create a dataset to create snapshots in and return its id
    private DatasetSummaryModel createTestDataset(String resourcePath) throws Exception {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId().toString());

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(datasetRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DatasetSummaryModel datasetSummaryModel =
                objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel.class);
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

    private BigQueryProject bigQueryProjectForDatasetName(String datasetName) {
        Dataset dataset = datasetDao.retrieveByName(datasetName);
        DatasetDataProject dataProject = dataLocationService.getProjectForDataset(dataset);
        return BigQueryProject.get(dataProject.getGoogleProjectId());
    }

    private void loadData(String datasetName,
                          String tableName,
                          String resourcePath,
                          FormatOptions options) throws Exception {
        String snapshotName = PDAO_PREFIX + datasetName;
        String location = "US";
        TableId tableId = TableId.of(snapshotName, tableName);
        BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetName);
        BigQuery bigQuery = bigQueryProject.getBigQuery();

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

    private SnapshotRequestModel makeSnapshotTestRequest(DatasetSummaryModel datasetSummaryModel,
                                                       String resourcePath) throws Exception {
        SnapshotRequestModel snapshotRequest = jsonLoader.loadObject(resourcePath, SnapshotRequestModel.class);
        // TODO SingleDatasetSnapshot
        snapshotRequest.getContents().get(0).getSource().setDatasetName(datasetSummaryModel.getName());
        snapshotRequest.profileId(datasetSummaryModel.getDefaultProfileId());
        return snapshotRequest;
    }

    private MvcResult launchCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
            throws Exception {
        snapshotOriginalName = snapshotRequest.getName();
        String snapshotName = Names.randomizeNameInfix(snapshotOriginalName, infix);
        snapshotRequest.setName(snapshotName);

        String jsonRequest = objectMapper.writeValueAsString(snapshotRequest);

        return mvc.perform(post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();
    }

    private MockHttpServletResponse performCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
        throws Exception {
        MvcResult result = launchCreateSnapshot(snapshotRequest, infix);
        MockHttpServletResponse response = validateJobModelAndWait(result);
        return response;
    }

    private SnapshotSummaryModel handleCreateSnapshotSuccessCase(SnapshotRequestModel snapshotRequest,
                                                               MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        if (!responseStatus.is2xxSuccessful()) {
            String failMessage = "createTestSnapshot failed: status=" + responseStatus.toString();
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

        SnapshotSummaryModel summaryModel = objectMapper.readValue(responseBody, SnapshotSummaryModel.class);
        createdSnapshotIds.add(summaryModel.getId());

        assertThat(summaryModel.getDescription(), equalTo(snapshotRequest.getDescription()));
        assertThat(summaryModel.getName(), equalTo(snapshotRequest.getName()));

        // Reset the name in the snapshot request
        snapshotRequest.setName(snapshotOriginalName);

        return summaryModel;
    }

    private ErrorModel handleCreateSnapshotFailureCase(MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        assertFalse("Expect create snapshot failure", responseStatus.is2xxSuccessful());

        assertTrue("Error model was returned on failure",
                StringUtils.contains(responseBody, "message"));

        return objectMapper.readValue(responseBody, ErrorModel.class);
    }

    private void deleteTestSnapshot(String id) throws Exception {
        MvcResult result = mvc.perform(delete("/api/repository/v1/snapshots/" + id)).andReturn();
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

    private EnumerateSnapshotModel enumerateTestSnapshots() throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/snapshots?offset=0&limit=1000"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        EnumerateSnapshotModel summary =
                objectMapper.readValue(response.getContentAsString(), EnumerateSnapshotModel.class);
        return summary;
    }

    private SnapshotModel getTestSnapshot(String id,
                                        SnapshotRequestModel snapshotRequest,
                                        DatasetSummaryModel datasetSummary) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/snapshots/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        SnapshotModel snapshotModel = objectMapper.readValue(response.getContentAsString(), SnapshotModel.class);

        assertThat(snapshotModel.getDescription(), equalTo(snapshotRequest.getDescription()));
        assertThat(snapshotModel.getName(), startsWith(snapshotRequest.getName()));

        assertThat("source array has one element",
                snapshotModel.getSource().size(), equalTo(1));
        SnapshotSourceModel sourceModel = snapshotModel.getSource().get(0);
        assertThat("snapshot dataset summary is the same as from dataset",
                sourceModel.getDataset(), equalTo(datasetSummary));

        return snapshotModel;
    }

    private void getNonexistentSnapshot(String id) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/snapshots/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        ErrorModel errorModel = objectMapper.readValue(response.getContentAsString(), ErrorModel.class);
        assertThat("proper not found error", errorModel.getMessage(), startsWith("Snapshot not found"));
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
                    fail("invalid response status " + status);
            }
        }
    }

    // Get the count of rows in a table or view
    private long queryForCount(
        String snapshotName,
        String tableName,
        BigQueryProject bigQueryProject) throws Exception {
        String bigQueryProjectId = bigQueryProject.getProjectId();
        BigQuery bigQuery = bigQueryProject.getBigQuery();
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(bigQueryProjectId).append('.').append(snapshotName).append('.').append(tableName).append('`');
        String sql = builder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        return countValue.getLongValue();
    }

}
