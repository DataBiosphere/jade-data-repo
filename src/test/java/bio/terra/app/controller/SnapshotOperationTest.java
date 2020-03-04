package bio.terra.app.controller;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.stringtemplate.v4.ST;

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
    @Autowired private ConnectedTestConfiguration testConfig;

    @MockBean
    private IamService samService;

    private List<String> createdSnapshotIds;
    private List<String> createdDatasetIds;
    private String snapshotOriginalName;
    private BillingProfile billingProfile;
    private Storage storage = StorageOptions.getDefaultInstance().getService();

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
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

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
    public void testProvidedIdsHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getName(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotProvidedIdsRequestModel snapshotRequest =
            makeSnapshotProvidedIdsTestRequest(datasetSummary, "snapshot-provided-ids-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshotProvidedIds(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = handleCreateSnapshotProvidedIdsSuccessCase(snapshotRequest, response);

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
        BigQueryProject bigQueryProject = TestUtils.bigQueryProjectForDatasetName(
            datasetDao, dataLocationService, datasetSummary.getName());
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
        BigQueryProject bigQueryProject = TestUtils.bigQueryProjectForDatasetName(
            datasetDao, dataLocationService, datasetSummary.getName());
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
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");

        // Other unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same snapshot summary that we made.
        List<SnapshotSummaryModel> snapshotList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_en_");
            SnapshotSummaryModel summaryModel = handleCreateSnapshotSuccessCase(snapshotRequest, response);
            snapshotList.add(summaryModel);
        }

        List<UUID> snapshotIds = snapshotList
            .stream()
            .map(snapshot -> UUID.fromString(snapshot.getId()))
            .collect(Collectors.toList());

        when(samService.listAuthorizedResources(any(), any())).thenReturn(snapshotIds);
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
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");
        SnapshotRequestModel badDataRequest = makeSnapshotTestRequest(datasetSummary,
                "snapshot-test-snapshot-baddata.json");

        MockHttpServletResponse response = performCreateSnapshot(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    private DatasetSummaryModel setupMinimalDataset() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("dataset-minimal.json");
        loadCsvData(datasetSummary.getId(), "participant", "dataset-minimal-participant.csv");
        loadCsvData(datasetSummary.getId(), "sample", "dataset-minimal-sample.csv");
        return  datasetSummary;
    }

    private DatasetSummaryModel setupArrayStructDataset() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("dataset-array-struct.json");
        loadJsonData(datasetSummary.getId(), "participant", "dataset-array-struct-participant.json");
        loadJsonData(datasetSummary.getId(), "sample", "dataset-array-struct-sample.json");
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
                .content(TestUtils.mapToJson(datasetRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DatasetSummaryModel datasetSummaryModel =
                TestUtils.mapFromJson(response.getContentAsString(), DatasetSummaryModel.class);
        createdDatasetIds.add(datasetSummaryModel.getId());

        return datasetSummaryModel;
    }

    private void loadCsvData(String datasetId, String tableName, String resourcePath) throws Exception {
        loadData(datasetId, tableName, resourcePath, IngestRequestModel.FormatEnum.CSV);
    }

    private void loadJsonData(String datasetId, String tableName, String resourcePath) throws Exception {
        loadData(datasetId, tableName, resourcePath, IngestRequestModel.FormatEnum.JSON);
    }

    private void loadData(String datasetId,
                          String tableName,
                          String resourcePath,
                          IngestRequestModel.FormatEnum format) throws Exception {

        String bucket = testConfig.getIngestbucket();
        BlobInfo stagingBlob = BlobInfo.newBuilder(bucket, resourcePath).build();
        byte[] data = IOUtils.toByteArray(jsonLoader.getClassLoader().getResource(resourcePath));

        IngestRequestModel ingestRequest = new IngestRequestModel()
            .table(tableName)
            .format(format)
            .path("gs://" + stagingBlob.getBucket() + "/" + stagingBlob.getName());

        if (format.equals(IngestRequestModel.FormatEnum.CSV)) {
            ingestRequest.csvSkipLeadingRows(1);
        }

        try {
            storage.create(stagingBlob, data);
            connectedOperations.ingestTableSuccess(datasetId, ingestRequest);
        } finally {
            storage.delete(stagingBlob.getBlobId());
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

    private SnapshotProvidedIdsRequestModel makeSnapshotProvidedIdsTestRequest(DatasetSummaryModel datasetSummaryModel,
                                                                    String resourcePath) throws Exception {
        SnapshotProvidedIdsRequestModel snapshotRequest = jsonLoader
            .loadObject(resourcePath, SnapshotProvidedIdsRequestModel.class);
        snapshotRequest.profileId(datasetSummaryModel.getDefaultProfileId());
        List<SnapshotProvidedIdsRequestContentsModel> snapshotContents = snapshotRequest.getContents();
        snapshotContents.get(0).setDatasetName(datasetSummaryModel.getName());
        snapshotRequest.setContents(snapshotContents);
        return snapshotRequest;
    }

    private MvcResult launchCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
        throws Exception {
        snapshotOriginalName = snapshotRequest.getName();
        String snapshotName = Names.randomizeNameInfix(snapshotOriginalName, infix);
        snapshotRequest.setName(snapshotName);

        String jsonRequest = TestUtils.mapToJson(snapshotRequest);

        return mvc.perform(post("/api/repository/v1/snapshots")
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    }

    private MvcResult launchCreateSnapshotProvidedIds(SnapshotProvidedIdsRequestModel snapshotRequest, String infix)
        throws Exception {
        snapshotOriginalName = snapshotRequest.getName();

        String snapshotName = Names.randomizeNameInfix(snapshotOriginalName, infix);
        snapshotRequest.setName(snapshotName);

        String jsonRequest = objectMapper.writeValueAsString(snapshotRequest);

        return mvc.perform(post("/api/repository/v1/snapshots/ids")
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();
    }

    private MockHttpServletResponse performCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
        throws Exception {
        MvcResult result = launchCreateSnapshot(snapshotRequest, infix);
        MockHttpServletResponse response = validateJobModelAndWait(result);
        return response;
    }

    private MockHttpServletResponse performCreateSnapshotProvidedIds(
        SnapshotProvidedIdsRequestModel snapshotRequest,
        String infix)
        throws Exception {
        MvcResult result = launchCreateSnapshotProvidedIds(snapshotRequest, infix);
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

    private SnapshotSummaryModel handleCreateSnapshotProvidedIdsSuccessCase(
        SnapshotProvidedIdsRequestModel snapshotRequest,
        MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        if (!responseStatus.is2xxSuccessful()) {
            String failMessage = "createTestSnapshot failed: status=" + responseStatus.toString();
            if (StringUtils.contains(responseBody, "message")) {
                // If the responseBody contains the word 'message', then we try to decode it as an ErrorModel
                // so we can generate good failure information.
                ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
                failMessage += " msg=" + errorModel.getMessage();
            } else {
                failMessage += " responseBody=" + responseBody;
            }
            fail(failMessage);
        }

        SnapshotSummaryModel summaryModel = TestUtils.mapFromJson(responseBody, SnapshotSummaryModel.class);
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

        return TestUtils.mapFromJson(responseBody, ErrorModel.class);
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
            TestUtils.mapFromJson(response.getContentAsString(), DeleteResponseModel.class);
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
                TestUtils.mapFromJson(response.getContentAsString(), EnumerateSnapshotModel.class);
        return summary;
    }

    private SnapshotModel getTestSnapshot(String id,
                                        SnapshotRequestModel snapshotRequest,
                                        DatasetSummaryModel datasetSummary) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/snapshots/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        SnapshotModel snapshotModel = TestUtils.mapFromJson(response.getContentAsString(), SnapshotModel.class);

        assertThat(snapshotModel.getDescription(), equalTo(snapshotRequest.getDescription()));
        assertThat(snapshotModel.getName(), startsWith(snapshotRequest.getName()));

        assertThat("source array has one element",
                snapshotModel.getSource().size(), equalTo(1));
        SnapshotSourceModel sourceModel = snapshotModel.getSource().get(0);
        assertThat("snapshot dataset summary is the same as from dataset",
                sourceModel.getDataset(), equalTo(datasetSummary));

        return snapshotModel;
    }

    private SnapshotModel getTestSnapshot(String id,
                                          SnapshotProvidedIdsRequestModel snapshotRequest,
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
        ErrorModel errorModel = TestUtils.mapFromJson(response.getContentAsString(), ErrorModel.class);
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

            JobModel jobModel = TestUtils.mapFromJson(response.getContentAsString(), JobModel.class);
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

    private static final String queryForCountTemplate =
        "SELECT COUNT(*) FROM `<project>.<snapshot>.<table>`";

    // Get the count of rows in a table or view
    private long queryForCount(
        String snapshotName,
        String tableName,
        BigQueryProject bigQueryProject) throws Exception {
        String bigQueryProjectId = bigQueryProject.getProjectId();
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        ST sqlTemplate = new ST(queryForCountTemplate);
        sqlTemplate.add("project", bigQueryProjectId);
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("table", tableName);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        return countValue.getLongValue();
    }

}
