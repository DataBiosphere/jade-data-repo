package bio.terra.service.dataset;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
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

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DatasetConnectedTest {
    @Autowired private MockMvc mvc;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private BigQueryPdao bigQueryPdao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private DatasetDao datasetDao;
    @Autowired private ConfigurationService configService;
    @Autowired private ConnectedTestConfiguration testConfig;
    @MockBean private IamProviderInterface samService;

    private BillingProfileModel billingProfile;
    private DatasetRequestModel datasetRequest;
    private DatasetSummaryModel summaryModel;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        configService.reset();
        billingProfile =
            connectedOperations.createProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());
        summaryModel = connectedOperations.createDataset(datasetRequest);
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
        configService.reset();
    }

    @Test
    public void testDuplicateName() throws Exception {
        assertNotNull("created dataset successfully the first time", summaryModel);

        // fetch the dataset and confirm the metadata matches the request
        DatasetModel datasetModel = connectedOperations.getDataset(summaryModel.getId());
        assertNotNull("fetched dataset successfully after creation", datasetModel);
        assertEquals("fetched dataset name matches request", datasetRequest.getName(), datasetModel.getName());

        // check that the dataset metadata row is unlocked
        String exclusiveLock = datasetDao.getExclusiveLock(UUID.fromString(summaryModel.getId()));
        assertNull("dataset row is unlocked", exclusiveLock);

        // try to create the same dataset again and check that it fails
        ErrorModel errorModel =
            connectedOperations.createDatasetExpectError(datasetRequest, HttpStatus.BAD_REQUEST);
        assertThat("error message includes name conflict",
            errorModel.getMessage(), containsString("Dataset name already exists"));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testOverlappingDeletes() throws Exception {
        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the dataset
        MvcResult result1 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // try to delete the dataset again
        MvcResult result2 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // disable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check the response from the first delete request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
        assertEquals("First delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // check that the second delete failed with a lock exception
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        ErrorModel errorModel2 = connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel2.getMessage(),
            startsWith("Failed to lock the dataset"));

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testSharedLockFileIngest() throws Exception {
        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in IngestFileIdStep
        configService.setFault(ConfigEnum.FILE_INGEST_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to ingest a file
        URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt",
            null, null);
        String targetPath1 = "/mm/" + Names.randomizeName("testdir") + "/testfile1.txt";
        FileLoadModel fileLoadModel1 = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 1")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
        MvcResult result1 = mvc.perform(post("/api/repository/v1/datasets/" + summaryModel.getId() + "/files")
            .contentType(MediaType.APPLICATION_JSON)
            .content(TestUtils.mapToJson(fileLoadModel1)))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has a shared lock
        // note: asserts are below outside the hang block
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

        // try to ingest a separate file
        String targetPath2 = "/mm/" + Names.randomizeName("testdir") + "/testfile2.txt";
        FileLoadModel fileLoadModel2 = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 2")
            .mimeType("text/plain")
            .targetPath(targetPath2)
            .profileId(billingProfile.getId());
        MvcResult result2 = mvc.perform(post("/api/repository/v1/datasets/" + summaryModel.getId() + "/files")
            .contentType(MediaType.APPLICATION_JSON)
            .content(TestUtils.mapToJson(fileLoadModel2)))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has two shared locks
        // note: asserts are below outside the hang block
        String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

        // try to delete the dataset, this should fail with a lock exception
        // note: asserts are below outside the hang block
        MvcResult result3 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // disable hang in IngestFileIdStep
        configService.setFault(ConfigEnum.FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has a shared lock during the first ingest request
        assertNull("dataset row has no exclusive lock", exclusiveLock1);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks1);
        assertEquals("dataset row has one shared lock", 1, sharedLocks1.length);

        // check that the dataset metadata row has two shared locks while both ingests are running
        assertNull("dataset row has no exclusive lock", exclusiveLock2);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks2);
        assertEquals("dataset row has two shared locks", 2, sharedLocks2.length);

        // check the response from the first ingest request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        FileModel fileModel1 = connectedOperations.handleSuccessCase(response1, FileModel.class);
        assertEquals("file description 1 correct", fileLoadModel1.getDescription(), fileModel1.getDescription());

        // check the response from the second ingest request
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        FileModel fileModel2 = connectedOperations.handleSuccessCase(response2, FileModel.class);
        assertEquals("file description 2 correct", fileLoadModel2.getDescription(), fileModel2.getDescription());

        // check the response from the delete request, confirm fails with a lock exception
        MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
        ErrorModel errorModel3 = connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel3.getMessage(),
            startsWith("Failed to lock the dataset"));

        // delete the dataset again and check that it succeeds now that there are no outstanding locks
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testSharedLockFileDelete() throws Exception {
        // ingest two files
        URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt",
            null, null);
        String targetPath1 = "/mm/" + Names.randomizeName("testdir") + "/testfile1.txt";
        FileLoadModel fileLoadModel1 = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 1")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
        FileModel fileModel1 = connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel1);

        String targetPath2 = "/mm/" + Names.randomizeName("testdir") + "/testfile2.txt";
        FileLoadModel fileLoadModel2 = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 2")
            .mimeType("text/plain")
            .targetPath(targetPath2)
            .profileId(billingProfile.getId());
        FileModel fileModel2 = connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel2);

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteFileLookupStep
        configService.setFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the first file
        MvcResult result1 = mvc.perform(
            delete("/api/repository/v1/datasets/" + summaryModel.getId() + "/files/" + fileModel1.getFileId()))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has a shared lock
        // note: asserts are below outside the hang block
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

        // try to delete the second file
        MvcResult result2 = mvc.perform(
            delete("/api/repository/v1/datasets/" + summaryModel.getId() + "/files/" + fileModel2.getFileId()))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has two shared locks
        // note: asserts are below outside the hang block
        String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

        // try to delete the dataset, this should fail with a lock exception
        // note: asserts are below outside the hang block
        MvcResult result3 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // disable hang in DeleteFileLookupStep
        configService.setFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has a shared lock during the first file delete
        assertNull("dataset row has no exclusive lock", exclusiveLock1);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks1);
        assertEquals("dataset row has one shared lock", 1, sharedLocks1.length);

        // check that the dataset metadata row has two shared locks while both file deletes are running
        assertNull("dataset row has no exclusive lock", exclusiveLock2);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks2);
        assertEquals("dataset row has two shared locks", 2, sharedLocks2.length);

        // check the response from the first delete file request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        assertEquals(response1.getStatus(), HttpStatus.OK.value());
        connectedOperations.checkDeleteResponse(response1);
        connectedOperations.removeFile(summaryModel.getId(), fileModel1.getFileId());

        // check the response from the second delete file request
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        assertEquals(response2.getStatus(), HttpStatus.OK.value());
        connectedOperations.checkDeleteResponse(response2);
        connectedOperations.removeFile(summaryModel.getId(), fileModel2.getFileId());

        // check that the delete request launched while the dataset had shared locks on it, failed with a lock exception
        MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
        ErrorModel errorModel3 = connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel3.getMessage(),
            startsWith("Failed to lock the dataset"));

        // delete the dataset again and check that it succeeds now that there are no outstanding locks
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testSharedLockTableIngest() throws Exception {
        // load a JSON file that contains the table rows to load into the test bucket
        String resourceFileName = "snapshot-test-dataset-data-without-rowids.json";
        String dirInCloud = "scratch/testSharedLockTableIngest/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob,
            IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in IngestSetupStep
        configService.setFault(ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off the first table ingest. it should hang in the cleanup step.
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;
        IngestRequestModel ingestRequest1 = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("thetable")
            .path(gsPath);
        MvcResult result1 = connectedOperations.ingestTableRaw(summaryModel.getId(), ingestRequest1);
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has a shared lock
        // note: asserts are below outside the hang block
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

        // kick off the second table ingest. it should hang in the cleanup step.
        IngestRequestModel ingestRequest2 = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("thetable")
            .path(gsPath);
        MvcResult result2 = connectedOperations.ingestTableRaw(summaryModel.getId(), ingestRequest2);
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has two shared locks
        // note: asserts are below outside the hang block
        String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

        // try to delete the dataset, this should fail with a lock exception
        // note: asserts are below outside the hang block
        MvcResult result3 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // disable hang in IngestSetupStep
        configService.setFault(ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has a shared lock during the first table ingest
        assertNull("dataset row has no exclusive lock", exclusiveLock1);
        assertEquals("dataset row has one shared lock", 1, sharedLocks1.length);

        // check that the dataset metadata row has two shared locks while both table ingests are running
        assertNull("dataset row has no exclusive lock", exclusiveLock2);
        assertEquals("dataset row has two shared locks", 2, sharedLocks2.length);

        // check the response from the first table ingest
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        connectedOperations.checkIngestTableResponse(response1);

        // check the response from the second delete table ingest
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        connectedOperations.checkIngestTableResponse(response2);

        // check that the delete launched while both the table ingests had shared locks taken out,
        // failed with a lock exception
        MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
        ErrorModel errorModel3 = connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel3.getMessage(),
            startsWith("Failed to lock the dataset"));

        // delete the dataset again and check that it succeeds now that there are no outstanding locks
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testRepeatedSoftDelete() throws Exception {
        // load a CSV file that contains the table rows to load into the test bucket
        String resourceFileName = "snapshot-test-dataset-data.csv";
        String dirInCloud = "scratch/testRepeatedSoftDelete/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob,
            IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
        String tableIngestInputFilePath =
            "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;

        // ingest the table
        String tableName = "thetable";
        IngestRequestModel ingestRequest = new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath);
        connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

        // load a CSV file that contains the table rows to soft delete into the test bucket
        String softDeleteRowId = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
        List<String> softDeleteRowIds = new ArrayList<>();
        softDeleteRowIds.add(softDeleteRowId); // add the same rowid twice
        softDeleteRowIds.add(softDeleteRowId);
        DataDeletionRequest softDeleteRequest = uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testRepeatedSoftDelete.csv", tableName, softDeleteRowIds);

        // make the soft delete request and wait for it to return
        connectedOperations.softDeleteSuccess(summaryModel.getId(), softDeleteRequest);

        // check that the size of the live table matches what we expect
        List<String> liveTableRowIds1 = getRowIdsFromBQTable(summaryModel.getName(), tableName);
        assertEquals("Size of live table is 3", 3, liveTableRowIds1.size());
        assertFalse("Soft deleted row id is not in live table", liveTableRowIds1.contains(softDeleteRowId));

        // note: the soft delete table name is not exposed to end users, so to check that the state of the
        // soft delete table is correct, I'm reaching into our internals to fetch the table name
        Dataset internalDatasetObj = datasetDao.retrieve(UUID.fromString(summaryModel.getId()));
        DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
        String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

        // check that the size of the soft delete table matches what we expect
        List<String> softDeleteRowIds1 = getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
        assertEquals("Size of soft delete table is 1", 1, softDeleteRowIds1.size());
        assertTrue("Soft deleted row id is in soft delete table", softDeleteRowIds1.contains(softDeleteRowId));

        // repeat the same soft delete request and wait for it to return
        connectedOperations.softDeleteSuccess(summaryModel.getId(), softDeleteRequest);

        // check that the size of the live table has not changed
        List<String> liveTableRowIds2 = getRowIdsFromBQTable(summaryModel.getName(), tableName);
        assertEquals("Size of live table is still 3", 3, liveTableRowIds2.size());
        assertFalse("Soft deleted row id is still not in live table", liveTableRowIds2.contains(softDeleteRowId));

        // check that the size of the soft delete table has not changed
        List<String> softDeleteRowIds2 = getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
        assertEquals("Size of soft delete table is still 1", 1, softDeleteRowIds2.size());
        assertTrue("Soft deleted row id is still in soft delete table", softDeleteRowIds2.contains(softDeleteRowId));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testConcurrentSoftDeletes() throws Exception {
        // load a CSV file that contains the table rows to load into the test bucket
        String resourceFileName = "snapshot-test-dataset-data.csv";
        String dirInCloud = "scratch/testConcurrentSoftDeletes/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob,
            IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
        String tableIngestInputFilePath =
            "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;

        // ingest the table
        String tableName = "thetable";
        IngestRequestModel ingestRequest = new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath);
        connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

        // load CSV file #1 that contains the table rows to soft delete into the test bucket
        String softDeleteRowId1 = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
        DataDeletionRequest softDeleteRequest1 = uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testConcurrentSoftDeletes1.csv", tableName, Collections.singletonList(softDeleteRowId1));

        // load CSV file #1 that contains the table rows to soft delete into the test bucket
        String softDeleteRowId2 = "13ae488a-e33f-4ee6-ba30-c1fca4d96b63";
        DataDeletionRequest softDeleteRequest2 = uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testConcurrentSoftDeletes2.csv", tableName, Collections.singletonList(softDeleteRowId2));

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DataDeletionStep
        configService.setFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off the first soft delete request, it should hang just before updating the soft delete table
        MvcResult softDeleteResult1 = connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest1);
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has a shared lock
        // note: asserts are below outside the hang block
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

        // kick off the second soft delete request, it should also hang just before updating the soft delete table
        MvcResult softDeleteResult2 = connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest2);
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has two shared locks
        // note: asserts are below outside the hang block
        String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
        String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

        // disable hang in DataDeletionStep
        configService.setFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has a shared lock after the first soft delete request was kicked off
        assertNull("dataset row has no exclusive lock", exclusiveLock1);
        assertEquals("dataset row has one shared lock", 1, sharedLocks1.length);

        // check that the dataset metadata row has two shared locks after the second soft delete request was kicked off
        assertNull("dataset row has no exclusive lock", exclusiveLock2);
        assertEquals("dataset row has two shared locks", 2, sharedLocks2.length);

        // wait for the first soft delete to finish and check it succeeded
        MockHttpServletResponse softDeleteResponse1 = connectedOperations.validateJobModelAndWait(softDeleteResult1);
        connectedOperations.handleSuccessCase(softDeleteResponse1, DeleteResponseModel.class);

        // wait for the second soft delete to finish and check it succeeded
        MockHttpServletResponse softDeleteResponse2 = connectedOperations.validateJobModelAndWait(softDeleteResult2);
        connectedOperations.handleSuccessCase(softDeleteResponse2, DeleteResponseModel.class);

        // check that the size of the live table matches what we expect
        List<String> liveTableRowIds = getRowIdsFromBQTable(summaryModel.getName(), tableName);
        assertEquals("Size of live table is 2", 2, liveTableRowIds.size());
        assertFalse("Soft deleted row id #1 is not in live table", liveTableRowIds.contains(softDeleteRowId1));
        assertFalse("Soft deleted row id #2 is not in live table", liveTableRowIds.contains(softDeleteRowId2));

        // note: the soft delete table name is not exposed to end users, so to check that the state of the
        // soft delete table is correct, I'm reaching into our internals to fetch the table name
        Dataset internalDatasetObj = datasetDao.retrieve(UUID.fromString(summaryModel.getId()));
        DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
        String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

        // check that the size of the soft delete table matches what we expect
        List<String> softDeleteRowIds = getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
        assertEquals("Size of soft delete table is 2", 2, softDeleteRowIds.size());
        assertTrue("Soft deleted row id #1 is in soft delete table", softDeleteRowIds.contains(softDeleteRowId1));
        assertTrue("Soft deleted row id #2 is in soft delete table", softDeleteRowIds.contains(softDeleteRowId2));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testBadSoftDelete() throws Exception {
        // load a CSV file that contains the table rows to load into the test bucket
        String resourceFileName = "snapshot-test-dataset-data.csv";
        String dirInCloud = "scratch/testBadSoftDelete/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob,
            IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
        String tableIngestInputFilePath =
            "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;

        // ingest the table
        String tableName = "thetable";
        IngestRequestModel ingestRequest = new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath);
        connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

        // load a CSV file that contains the table rows to soft delete into the test bucket
        String softDeleteBadRowId = "badrowid";
        String softDeleteGoodRowId = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
        List<String> softDeleteRowIds = new ArrayList<>();
        softDeleteRowIds.add(softDeleteBadRowId);
        softDeleteRowIds.add(softDeleteGoodRowId);
        DataDeletionRequest softDeleteRequest = uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testBadSoftDelete.csv", tableName, softDeleteRowIds);

        // make the soft delete request and wait for it to return
        MvcResult softDeleteResult = connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest);
        MockHttpServletResponse softDeleteResponse = connectedOperations.validateJobModelAndWait(softDeleteResult);
        assertEquals("soft delete of bad row id failed",
            HttpStatus.BAD_REQUEST.value(), softDeleteResponse.getStatus());

        // check that the size of the live table matches what we expect
        List<String> liveTableRowIds = getRowIdsFromBQTable(summaryModel.getName(), tableName);
        assertEquals("Size of live table is 4", 4, liveTableRowIds.size());
        assertFalse("Bad row id is not in live table", liveTableRowIds.contains(softDeleteBadRowId));
        assertTrue("Good row id is in live table", liveTableRowIds.contains(softDeleteGoodRowId));

        // note: the soft delete table name is not exposed to end users, so to check that the state of the
        // soft delete table is correct, I'm reaching into our internals to fetch the table name
        Dataset internalDatasetObj = datasetDao.retrieve(UUID.fromString(summaryModel.getId()));
        DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
        String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

        // check that the size of the soft delete table matches what we expect
        List<String> softDeleteRowIdsFromBQ = getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
        assertEquals("Size of soft delete table is 0", 0, softDeleteRowIdsFromBQ.size());
        assertFalse("Bad row id is not in soft delete table", softDeleteRowIdsFromBQ.contains(softDeleteBadRowId));
        assertFalse("Good row id is not in soft delete table", softDeleteRowIdsFromBQ.contains(softDeleteGoodRowId));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testExcludeLockedFromDatasetLookups() throws Exception {
        // check that the dataset metadata row is unlocked
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row is not exclusively locked", exclusiveLock);
        String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

        // retrieve the dataset and check that it finds it
        DatasetModel datasetModel = connectedOperations.getDataset(summaryModel.getId());
        assertEquals("Lookup unlocked dataset succeeds", summaryModel.getName(), datasetModel.getName());

        // enumerate datasets and check that this dataset is included in the set
        EnumerateDatasetModel enumerateDatasetModel = connectedOperations.enumerateDatasets(summaryModel.getName());
        List<DatasetSummaryModel> enumeratedDatasets = enumerateDatasetModel.getItems();
        boolean foundDatasetWithMatchingId = false;
        for (DatasetSummaryModel enumeratedDataset : enumeratedDatasets) {
            if (enumeratedDataset.getId().equals(summaryModel.getId())) {
                foundDatasetWithMatchingId = true;
                break;
            }
        }
        assertTrue("Unlocked included in enumeration", foundDatasetWithMatchingId);

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off a request to delete the dataset. this should hang before unlocking the dataset object.
        MvcResult deleteResult = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has an exclusive lock
        // note: asserts are below outside the hang block
        exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        sharedLocks = datasetDao.getSharedLocks(datasetId);

        // retrieve the dataset, should return not found
        // note: asserts are below outside the hang block
        MvcResult retrieveResult = mvc.perform(get("/api/repository/v1/datasets/" + datasetId)).andReturn();

        // enumerate datasets, this dataset should not be included in the set
        // note: asserts are below outside the hang block
        MvcResult enumerateResult = connectedOperations.enumerateDatasetsRaw(summaryModel.getName());

        // disable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has an exclusive lock after kicking off the delete
        assertNotNull("dataset row is exclusively locked", exclusiveLock);
        assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

        // check that the retrieve request returned not found
        connectedOperations.handleFailureCase(retrieveResult.getResponse(), HttpStatus.NOT_FOUND);

        // check that the enumerate request returned successfully and that this dataset is not included in the set
        enumerateDatasetModel =
            connectedOperations.handleSuccessCase(enumerateResult.getResponse(), EnumerateDatasetModel.class);
        enumeratedDatasets = enumerateDatasetModel.getItems();
        foundDatasetWithMatchingId = false;
        for (DatasetSummaryModel enumeratedDataset : enumeratedDatasets) {
            if (enumeratedDataset.getId().equals(summaryModel.getId())) {
                foundDatasetWithMatchingId = true;
                break;
            }
        }
        assertFalse("Exclusively locked not included in enumeration", foundDatasetWithMatchingId);

        // check the response from the delete request
        MockHttpServletResponse deleteResponse = connectedOperations.validateJobModelAndWait(deleteResult);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
        assertEquals("Dataset delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testExcludeLockedFromFileLookups() throws Exception {
        // check that the dataset metadata row is unlocked
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row is not exclusively locked", exclusiveLock);
        String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

        // ingest a file
        URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt",
            null, null);
        String targetPath1 = "/mm/" + Names.randomizeName("testdir") + "/testExcludeLockedFromFileLookups.txt";
        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("testExcludeLockedFromFileLookups")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
        FileModel fileModel = connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel);

        // lookup the file by id and check that it's found
        FileModel fileModelFromIdLookup =
            connectedOperations.lookupFileSuccess(summaryModel.getId(), fileModel.getFileId());
        assertEquals("File found by id lookup",
            fileModel.getDescription(), fileModelFromIdLookup.getDescription());

        // lookup the file by path and check that it's found
        FileModel fileModelFromPathLookup =
            connectedOperations.lookupFileByPathSuccess(summaryModel.getId(), fileModel.getPath(), -1);
        assertEquals("File found by path lookup", fileModel.getDescription(), fileModelFromPathLookup.getDescription());

        // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before disabling the hang
        // ====================================================
        // enable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // kick off a request to delete the dataset. this should hang before unlocking the dataset object.
        MvcResult deleteResult = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has an exclusive lock
        // note: asserts are below outside the hang block
        exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        sharedLocks = datasetDao.getSharedLocks(datasetId);

        // lookup the file by id and check that it's NOT found
        // note: asserts are below outside the hang block
        MockHttpServletResponse lookupFileByIdResponse =
            connectedOperations.lookupFileRaw(summaryModel.getId(), fileModel.getFileId());

        // lookup the file by path and check that it's NOT found
        // note: asserts are below outside the hang block
        MockHttpServletResponse lookupFileByPathResponse =
            connectedOperations.lookupFileByPathRaw(summaryModel.getId(), fileModel.getPath(), -1);

        // disable hang in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        // ====================================================

        // check that the dataset metadata row has an exclusive lock after kicking off the delete
        assertNotNull("dataset row is exclusively locked", exclusiveLock);
        assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

        // check that the lookup file by id returned not found
        assertEquals("File NOT found by id lookup", HttpStatus.NOT_FOUND,
            HttpStatus.valueOf(lookupFileByIdResponse.getStatus()));

        // check that the lookup file by path returned not found
        assertEquals("File NOT found by path lookup", HttpStatus.NOT_FOUND,
            HttpStatus.valueOf(lookupFileByPathResponse.getStatus()));

        // check the response from the delete request
        MockHttpServletResponse deleteResponse = connectedOperations.validateJobModelAndWait(deleteResult);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
        assertEquals("Dataset delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // remove the file from the connectedoperation bookkeeping list
        connectedOperations.removeFile(summaryModel.getId(), fileModel.getFileId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    private List<String> getRowIdsFromBQTable(String datasetName, String tableName) throws Exception {
        String rowIdColumn = PdaoConstant.PDAO_ROW_ID_COLUMN;
        TableResult bqQueryResult = TestUtils.selectFromBigQueryDataset(bigQueryPdao, datasetDao, dataLocationService,
            datasetName, tableName, rowIdColumn);
        List<String> rowIds = new ArrayList<>();
        bqQueryResult.iterateAll().forEach(r -> rowIds.add(r.get(rowIdColumn).getStringValue()));
        return rowIds;

    }

    private DataDeletionRequest uploadInputFileAndBuildSoftDeleteRequest(
        String dirInCloud, String filenameInCloud, String tableName, List<String> softDeleteRowIds) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // load a CSV file that contains the table rows to soft delete into the test bucket
        StringBuilder csvLines = new StringBuilder();
        for (String softDeleteRowId : softDeleteRowIds) {
            csvLines.append(softDeleteRowId + "\n");
        }
        BlobInfo softDeleteBlob = BlobInfo
            .newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + filenameInCloud)
            .build();
        storage.create(softDeleteBlob, csvLines.toString().getBytes(Charset.forName("UTF-8")));
        String softDeleteInputFilePath =
            "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + filenameInCloud;

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + filenameInCloud);

        // build the soft delete request with a pointer to a file that contains the row ids to soft delete
        DataDeletionGcsFileModel softDeleteGcsFileModel = new DataDeletionGcsFileModel()
            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
            .path(softDeleteInputFilePath);
        DataDeletionTableModel softDeleteTableModel = new DataDeletionTableModel()
            .tableName(tableName)
            .gcsFileSpec(softDeleteGcsFileModel);
        DataDeletionRequest softDeleteRequest = new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE)
            .tables(Arrays.asList(softDeleteTableModel));

        return softDeleteRequest;
    }
}
