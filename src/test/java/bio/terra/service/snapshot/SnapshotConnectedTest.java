package bio.terra.service.snapshot;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TableModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamProviderInterface;
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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class SnapshotConnectedTest {
    // TODO: MORE TESTS to be done when we can ingest data:
    // - test more complex datasets with relationships
    // - test relationship walking with valid and invalid setups
    // TODO: MORE TESTS when we separate the value translation from the create
    // - test invalid row ids

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private DatasetDao datasetDao;
    @Autowired private SnapshotDao snapshotDao;
    @Autowired private ProfileDao profileDao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private ConfigurationService configService;

    @MockBean
    private IamProviderInterface samService;

    private String snapshotOriginalName;
    private BillingProfileModel billingProfile;
    private Storage storage = StorageOptions.getDefaultInstance().getService();

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        billingProfile =
            connectedOperations.createProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void testHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);
        List<TableModel> tables = snapshotModel.getTables();
        assertThat("there's a table", tables.size(), equalTo(1));
        assertThat("rowCount is getting set correctly", tables.get(0).getRowCount(), equalTo(3));

        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());

        getNonexistentSnapshot(snapshotModel.getId());
    }

    @Test
    public void testRowIdsHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotRequestModel snapshotRequest =
            makeSnapshotTestRequest(datasetSummary, "snapshot-row-ids-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());

        getNonexistentSnapshot(snapshotModel.getId());
    }

    @Test
    public void testQueryHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotRequestModel snapshotRequest =
            makeSnapshotTestRequest(datasetSummary, "snapshot-query-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());

        getNonexistentSnapshot(snapshotModel.getId());
    }

    @Test
    public void testFullViewsHappyPath() throws Exception {
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        SnapshotRequestModel snapshotRequest =
            makeSnapshotTestRequest(datasetSummary, "snapshot-fullviews-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());

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
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);
        List<TableModel> tables = snapshotModel.getTables();
        Optional<TableModel> participantTable = tables
            .stream()
            .filter(t -> t.getName().equals("participant"))
            .findFirst();
        Optional<TableModel> sampleTable = tables
            .stream()
            .filter(t -> t.getName().equals("sample"))
            .findFirst();
        assertThat("participant table exists", participantTable.isPresent(), equalTo(true));
        assertThat("sample table exists", sampleTable.isPresent(), equalTo(true));
        long snapshotParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(1L));
        assertThat("participant row count matches expectation", participantTable.get().getRowCount(), equalTo(1));
        long snapshotSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", snapshotSamples, equalTo(2L));
        assertThat("sample row count matches expectation", sampleTable.get().getRowCount(), equalTo(2));
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
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
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
        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.NOT_FOUND.value()));
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
            SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
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
            connectedOperations.deleteTestSnapshot(enumeratedArray.get(i).getId());
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

    @Test
    public void testDuplicateName() throws Exception {
        // create a dataset and load some tabular data
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        // create a snapshot
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        // fetch the snapshot and confirm the metadata matches the request
        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);
        assertNotNull("fetched snapshot successfully after creation", snapshotModel);

        // check that the snapshot metadata row is unlocked
        SnapshotSummary snapshotSummary = snapshotDao.retrieveSummaryByName(snapshotModel.getName());
        assertNull("snapshot row is unlocked", snapshotSummary.getFlightId());

        // try to create the same snapshot again and check that it fails
        snapshotRequest.setName(snapshotModel.getName());
        response = performCreateSnapshot(snapshotRequest, null);
        ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
        assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));
        assertThat("error message includes name conflict",
            errorModel.getMessage(), containsString("Snapshot name already exists"));

        // delete and confirm deleted
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        getNonexistentSnapshot(snapshotModel.getId());
    }

    @Test
    public void testOverlappingDeletes() throws Exception {
        // create a dataset and load some tabular data
        DatasetSummaryModel datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");

        // create a snapshot
        SnapshotSummaryModel summaryModel = connectedOperations.createSnapshot(datasetSummary,
            "snapshot-test-snapshot.json", "_d2_");

        // enable wait in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the snapshot
        MvcResult result1 = mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();

        // try to delete the snapshot again
        MvcResult result2 = mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        ErrorModel errorModel2 = connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel2.getMessage(),
            startsWith("Failed to lock the snapshot"));

        // disable wait in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

        // check the response from the first delete request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
        assertEquals("First delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // confirm deleted
        getNonexistentSnapshot(summaryModel.getId());
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
        return connectedOperations.createDataset(billingProfile, resourcePath);
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
        BlobInfo stagingBlob = BlobInfo.newBuilder(bucket, UUID.randomUUID() + "-" + resourcePath).build();
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
        SnapshotRequestContentsModel content = snapshotRequest.getContents().get(0);
        // TODO SingleDatasetSnapshot
        String newDatasetName = datasetSummaryModel.getName();
        String origDatasetName = content.getDatasetName();
        // swap in the correct dataset name (with the id at the end)
        content.setDatasetName(newDatasetName);
        snapshotRequest.profileId(datasetSummaryModel.getDefaultProfileId());
        if (content.getMode().equals(SnapshotRequestContentsModel.ModeEnum.BYQUERY)) {
            // if its by query, also set swap in the correct dataset name in the query
            String query = content.getQuerySpec().getQuery();
            content.getQuerySpec().setQuery(query.replace(origDatasetName, newDatasetName));
        }
        return snapshotRequest;
    }

    private MvcResult launchCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
            throws Exception {
        if (infix != null) {
            String snapshotName = Names.randomizeNameInfix(snapshotRequest.getName(), infix);
            snapshotRequest.setName(snapshotName);
        }

        String jsonRequest = TestUtils.mapToJson(snapshotRequest);

        return mvc.perform(post("/api/repository/v1/snapshots")
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    }

    private MockHttpServletResponse performCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
        throws Exception {
        snapshotOriginalName = snapshotRequest.getName();
        MvcResult result = launchCreateSnapshot(snapshotRequest, infix);
        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
        return response;
    }

    private SnapshotSummaryModel validateSnapshotCreated(SnapshotRequestModel snapshotRequest,
                                                         MockHttpServletResponse response) throws Exception {
        SnapshotSummaryModel summaryModel = connectedOperations.handleCreateSnapshotSuccessCase(response);

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
