package bio.terra.service.snapshot;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
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
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
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
import org.hamcrest.CoreMatchers;
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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
    @Autowired private DrsIdService drsIdService;

    @MockBean
    private IamProviderInterface samService;

    private String snapshotOriginalName;
    private BillingProfileModel billingProfile;
    private Storage storage = StorageOptions.getDefaultInstance().getService();
    private DatasetSummaryModel datasetSummary;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        configService.reset();
        billingProfile =
            connectedOperations.createProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
        datasetSummary = createTestDataset("snapshot-test-dataset.json");
        loadCsvData(datasetSummary.getId(), "thetable", "snapshot-test-dataset-data.csv");
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
        configService.reset();
    }

    @Test
    public void testHappyPath() throws Exception {
        snapshotHappyPathTestingHelper("snapshot-test-snapshot.json");
    }

    @Test
    public void testFaultyPath() throws Exception {
        // Run the happy path test, but insert the GRANT ACCESS faults to simulate IAM propagation failures
        configService.setFault(ConfigEnum.DATASET_GRANT_ACCESS_FAULT.name(), true);
        configService.setFault(ConfigEnum.SNAPSHOT_GRANT_ACCESS_FAULT.name(), true);
        testHappyPath();
    }

    @Test
    public void testRowIdsHappyPath() throws Exception {
        snapshotHappyPathTestingHelper("snapshot-row-ids-test-snapshot.json");
    }

    @Test
    public void testQueryHappyPath() throws Exception {
        snapshotHappyPathTestingHelper("snapshot-query-test-snapshot.json");
    }

    @Test
    public void testFullViewsHappyPath() throws Exception {
        snapshotHappyPathTestingHelper("snapshot-fullviews-test-snapshot.json");
    }

    @Test
    public void testMinimal() throws Exception {
        DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();
        String datasetName = PDAO_PREFIX + datasetMinimalSummary.getName();
        BigQueryProject bigQueryProject = TestUtils.bigQueryProjectForDatasetName(
            datasetDao, dataLocationService, datasetMinimalSummary.getName());
        long datasetParticipants = queryForCount(datasetName, "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetMinimalSummary,
                "dataset-minimal-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetMinimalSummary);
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
        DatasetSummaryModel datasetArraySummary = setupArrayStructDataset();
        String datasetName = PDAO_PREFIX + datasetArraySummary.getName();
        BigQueryProject bigQueryProject = TestUtils.bigQueryProjectForDatasetName(
            datasetDao, dataLocationService, datasetArraySummary.getName());
        long datasetParticipants = queryForCount(datasetName, "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(datasetName, "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetArraySummary,
            "snapshot-array-struct.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
        getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetArraySummary);

        long snapshotParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(2L));
        long snapshotSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("dataset samples loaded properly", snapshotSamples, equalTo(3L));
    }

    @Test
    public void testMinimalBadAsset() throws Exception {
        DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetMinimalSummary,
                "dataset-minimal-snapshot-bad-asset.json");
        MvcResult result = launchCreateSnapshot(snapshotRequest, "");
        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.NOT_FOUND.value()));
    }

    @Test
    public void testEnumeration() throws Exception {
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
        SnapshotRequestModel badDataRequest = makeSnapshotTestRequest(datasetSummary,
                "snapshot-test-snapshot-baddata.json");

        MockHttpServletResponse response = performCreateSnapshot(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    @Test
    public void testDuplicateName() throws Exception {
        // create a snapshot
        SnapshotRequestModel snapshotRequest = makeSnapshotTestRequest(datasetSummary, "snapshot-test-snapshot.json");
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        // fetch the snapshot and confirm the metadata matches the request
        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);
        assertNotNull("fetched snapshot successfully after creation", snapshotModel);

        // check that the snapshot metadata row is unlocked
        String exclusiveLock = snapshotDao.getExclusiveLockState(UUID.fromString(snapshotModel.getId()));
        assertNull("snapshot row is unlocked", exclusiveLock);

        // try to create the same snapshot again and check that it fails
        snapshotRequest.setName(snapshotModel.getName());
        response = performCreateSnapshot(snapshotRequest, null);
        ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
        assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));
        assertThat("error message includes name conflict",
            errorModel.getMessage(), containsString("Snapshot name already exists"));

        // delete and confirm deleted
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testOverlappingDeletes() throws Exception {
        // create a snapshot
        SnapshotSummaryModel summaryModel = connectedOperations.createSnapshot(datasetSummary,
            "snapshot-test-snapshot.json", "_d2_");

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the snapshot
        MvcResult result1 = mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();

        // try to delete the snapshot again, this should fail with a lock exception
        // note: asserts are below outside the hang block
        MvcResult result2 = mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();

        // disable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check the response from the first delete request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
        assertEquals("First delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // check the response from the second delete request
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        ErrorModel errorModel2 = connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel2.getMessage(),
            startsWith("Failed to lock the snapshot"));

        // confirm deleted
        connectedOperations.getSnapshotExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testExcludeLockedFromSnapshotLookups() throws Exception {
        // create a snapshot
        SnapshotSummaryModel snapshotSummary = connectedOperations.createSnapshot(datasetSummary,
            "snapshot-test-snapshot.json", "_d2_");

        // check that the snapshot metadata row is unlocked
        String exclusiveLock = snapshotDao.getExclusiveLockState(UUID.fromString(snapshotSummary.getId()));
        assertNull("snapshot row is unlocked", exclusiveLock);

        // retrieve the snapshot and check that it finds it
        SnapshotModel snapshotModel = connectedOperations.getSnapshot(snapshotSummary.getId());
        assertEquals("Lookup unlocked snapshot succeeds", snapshotSummary.getName(), snapshotModel.getName());

        // enumerate snapshots and check that this snapshot is included in the set
        EnumerateSnapshotModel enumerateSnapshotModelModel =
            connectedOperations.enumerateSnapshots(snapshotSummary.getName());
        List<SnapshotSummaryModel> enumeratedSnapshots = enumerateSnapshotModelModel.getItems();
        boolean foundSnapshotWithMatchingId = false;
        for (SnapshotSummaryModel enumeratedSnapshot : enumeratedSnapshots) {
            if (enumeratedSnapshot.getId().equals(snapshotSummary.getId())) {
                foundSnapshotWithMatchingId = true;
                break;
            }
        }
        assertTrue("Unlocked included in enumeration", foundSnapshotWithMatchingId);

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off a request to delete the snapshot. this should hang before unlocking the snapshot object.
        MvcResult deleteResult =
            mvc.perform(delete("/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // note: asserts are below outside the hang block
        exclusiveLock = snapshotDao.getExclusiveLockState(UUID.fromString(snapshotSummary.getId()));

        // retrieve the snapshot and check that it returns not found
        // note: asserts are below outside the hang block
        MvcResult retrieveResult =
            mvc.perform(get("/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();

        // enumerate snapshots and check that this snapshot is not included in the set
        // note: asserts are below outside the hang block
        MvcResult enumerateResult = connectedOperations.enumerateSnapshotsRaw(snapshotSummary.getName());

        // disable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the snapshot metadata row has an exclusive lock after kicking off the delete
        assertNotNull("snapshot row is exclusively locked", exclusiveLock);

        // check that the retrieve snapshot returned not found
        connectedOperations.handleFailureCase(retrieveResult.getResponse(), HttpStatus.NOT_FOUND);

        // check that the enumerate snapshots returned successfully and that this snapshot is not included in the set
        enumerateSnapshotModelModel =
            connectedOperations.handleSuccessCase(enumerateResult.getResponse(), EnumerateSnapshotModel.class);

        enumeratedSnapshots = enumerateSnapshotModelModel.getItems();
        foundSnapshotWithMatchingId = false;
        for (SnapshotSummaryModel enumeratedSnapshot : enumeratedSnapshots) {
            if (enumeratedSnapshot.getId().equals(snapshotSummary.getId())) {
                foundSnapshotWithMatchingId = true;
                break;
            }
        }
        assertFalse("Exclusively locked not included in enumeration", foundSnapshotWithMatchingId);

        // check the response from the delete request
        MockHttpServletResponse deleteResponse = connectedOperations.validateJobModelAndWait(deleteResult);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
        assertEquals("Snapshot delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // try to fetch the snapshot again and confirm nothing is returned
        connectedOperations.getSnapshotExpectError(snapshotSummary.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testExcludeLockedFromSnapshotFileLookups() throws Exception {
        // create a dataset
        DatasetSummaryModel datasetRefSummary = createTestDataset("simple-with-filerefs-dataset.json");

        // ingest a file
        URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt",
            null, null);
        String targetFilePath =
            "/mm/" + Names.randomizeName("testdir") + "/testExcludeLockedFromSnapshotFileLookups.txt";
        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("testExcludeLockedFromSnapshotFileLookups")
            .mimeType("text/plain")
            .targetPath(targetFilePath)
            .profileId(billingProfile.getId());
        FileModel fileModel = connectedOperations.ingestFileSuccess(datasetRefSummary.getId(), fileLoadModel);

        // generate a JSON file with the fileref
        String jsonLine = "{\"name\":\"name1\", \"file_ref\":\"" + fileModel.getFileId() + "\"}\n";

        // load a JSON file that contains the table rows to load into the test bucket
        String jsonFileName = "this-better-pass.json";
        String dirInCloud = "scratch/testExcludeLockedFromSnapshotFileLookups/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + jsonFileName)
            .build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob, jsonLine.getBytes(StandardCharsets.UTF_8));

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + jsonFileName);

        // ingest the tabular data from the JSON file we just generated
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + jsonFileName;
        IngestRequestModel ingestRequest1 = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("tableA")
            .path(gsPath);
        connectedOperations.ingestTableSuccess(datasetRefSummary.getId(), ingestRequest1);

        // create a snapshot
        SnapshotSummaryModel snapshotSummary = connectedOperations.createSnapshot(
            datasetRefSummary, "simple-with-filerefs-snapshot.json", "");

        // check that the snapshot metadata row is unlocked
        String exclusiveLock = snapshotDao.getExclusiveLockState(UUID.fromString(snapshotSummary.getId()));
        assertNull("snapshot row is unlocked", exclusiveLock);

        String fileUri = getFileRefIdFromSnapshot(snapshotSummary);
        DrsId drsId = drsIdService.fromUri(fileUri);
        DRSObject drsObject = connectedOperations.drsGetObjectSuccess(drsId.toDrsObjectId(), false);
        String filePath = drsObject.getAliases().get(0);

        // lookup the snapshot file by DRS id, make sure it's returned (lookupSnapshotFileSuccess will already check)
        FileModel fsObjById =
            connectedOperations.lookupSnapshotFileSuccess(snapshotSummary.getId(), drsId.getFsObjectId());
        assertEquals("Retrieve snapshot file by DRS id matches desc", fsObjById.getDescription(),
            fileLoadModel.getDescription());

        // lookup the snapshot file by DRS path and check that it's found
        FileModel fsObjByPath =
            connectedOperations.lookupSnapshotFileByPathSuccess(snapshotSummary.getId(), filePath, 0);
        assertEquals("Retrieve snapshot file by path matches desc",
            fsObjByPath.getDescription(), fileLoadModel.getDescription());
        assertThat("Retrieve snapshot file objects match", fsObjById, CoreMatchers.equalTo(fsObjByPath));

        // now the snapshot exists....let's get it locked!

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off a request to delete the snapshot. this should hang before unlocking the snapshot object.
        // note: asserts are below outside the hang block
        MvcResult deleteResult = mvc.perform(delete(
            "/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch and get to the hang

        // check that the snapshot metadata row has an exclusive lock
        exclusiveLock = snapshotDao.getExclusiveLockState(UUID.fromString(snapshotSummary.getId()));

        // lookup the snapshot file by id and check that it's NOT found
        MockHttpServletResponse failedGetSnapshotByIdResponse =
            connectedOperations.lookupSnapshotFileRaw(snapshotSummary.getId(), drsId.getFsObjectId());

        // lookup the snapshot file by path and check that it's NOT found
        MockHttpServletResponse failedGetSnapshotByPathResponse =
            connectedOperations.lookupSnapshotFileByPathRaw(snapshotSummary.getId(), filePath, 0);

        // disable hang in DeleteSnapshotPrimaryDataStep
        configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the snapshot metadata row has an exclusive lock after kicking off the delete
        assertNotNull("snapshot row is exclusively locked", exclusiveLock);

        assertEquals("Snapshot file NOT found by DRS id lookup",
            HttpStatus.NOT_FOUND, HttpStatus.valueOf(failedGetSnapshotByIdResponse.getStatus()));

        assertEquals("Snapshot file NOT found by path lookup",
            HttpStatus.NOT_FOUND, HttpStatus.valueOf(failedGetSnapshotByPathResponse.getStatus()));

        // check the response from the snapshot delete request
        MockHttpServletResponse deleteResponse = connectedOperations.validateJobModelAndWait(deleteResult);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
        assertEquals("Snapshot delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(datasetRefSummary.getId());

        // remove the file from the connectedoperation bookkeeping list
        connectedOperations.removeFile(datasetRefSummary.getId(), fileModel.getFileId());

        // try to fetch the snapshot again and confirm nothing is returned
        connectedOperations.getSnapshotExpectError(snapshotSummary.getId(), HttpStatus.NOT_FOUND);

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(datasetRefSummary.getId(), HttpStatus.NOT_FOUND);
    }

    private DatasetSummaryModel setupMinimalDataset() throws Exception {
        DatasetSummaryModel datasetMinimalSummary = createTestDataset("dataset-minimal.json");
        loadCsvData(datasetMinimalSummary.getId(), "participant", "dataset-minimal-participant.csv");
        loadCsvData(datasetMinimalSummary.getId(), "sample", "dataset-minimal-sample.csv");
        return  datasetMinimalSummary;
    }

    private DatasetSummaryModel setupArrayStructDataset() throws Exception {
        DatasetSummaryModel datasetArraySummary = createTestDataset("dataset-array-struct.json");
        loadJsonData(datasetArraySummary.getId(), "participant", "dataset-array-struct-participant.json");
        loadJsonData(datasetArraySummary.getId(), "sample", "dataset-array-struct-sample.json");
        return  datasetArraySummary;
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

    private static final String queryForRefIdTemplate =
        "SELECT file_ref FROM `<project>.<snapshot>.<table>` WHERE file_ref IS NOT NULL";

    // Technically a helper method, but so specific to testExcludeLockedFromSnapshotFileLookups, likely not re-useable
    private String getFileRefIdFromSnapshot(SnapshotSummaryModel snapshotSummary) throws InterruptedException {
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotSummary.getName());
        SnapshotDataProject dataProject = dataLocationService.getOrCreateProject(snapshot);
        BigQueryProject bigQueryProject = BigQueryProject.get(dataProject.getGoogleProjectId());
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        ST sqlTemplate = new ST(queryForRefIdTemplate);
        sqlTemplate.add("project", dataProject.getGoogleProjectId());
        sqlTemplate.add("snapshot", snapshot.getName());
        sqlTemplate.add("table", "tableA");

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue idValue = row.get(0);
        return idValue.getStringValue();
    }

    private void snapshotHappyPathTestingHelper(String path) throws Exception {
        SnapshotRequestModel snapshotRequest =
            makeSnapshotTestRequest(datasetSummary, path);
        MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_thp_");
        SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

        SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId(), snapshotRequest, datasetSummary);

        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        // Duplicate delete should work
        connectedOperations.deleteTestSnapshot(snapshotModel.getId());
        connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);
    }

}
