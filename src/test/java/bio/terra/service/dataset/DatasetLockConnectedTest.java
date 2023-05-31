package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

public class DatasetLockConnectedTest {

  @Autowired private MockMvc mvc;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private DatasetDao datasetDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private DatasetSummaryModel summaryModel;
  private static final Logger logger = LoggerFactory.getLogger(DatasetLockConnectedTest.class);

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // create a dataset and check that it succeeds
    String resourcePath = "snapshot-test-dataset.json";
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId())
        .dedicatedIngestServiceAccount(false);
    summaryModel = connectedOperations.createDataset(datasetRequest);
    logger.info("--------begin test---------");
  }

  @After
  public void tearDown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  @Test
  public void testSharedLockFileIngest() throws Exception {
    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in IngestFileIdStep
    configService.setFault(ConfigEnum.FILE_INGEST_LOCK_CONFLICT_STOP_FAULT.name(), true);
    // Make sure that dataset delete fails on lock conflict
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_SKIP_RETRY_FAULT.name(), true);

    // try to ingest a file
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetPath1 = "/mm/" + Names.randomizeName("testdir") + "/testfile1.txt";
    FileLoadModel fileLoadModel1 =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 1")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
    MvcResult result1 =
        mvc.perform(
                post("/api/repository/v1/datasets/" + summaryModel.getId() + "/files")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(fileLoadModel1)))
            .andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has a shared lock
    // note: asserts are below outside the hang block
    UUID datasetId = UUID.fromString(summaryModel.getId().toString());
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

    // try to ingest a separate file
    String targetPath2 = "/mm/" + Names.randomizeName("testdir") + "/testfile2.txt";
    FileLoadModel fileLoadModel2 =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 2")
            .mimeType("text/plain")
            .targetPath(targetPath2)
            .profileId(billingProfile.getId());
    MvcResult result2 =
        mvc.perform(
                post("/api/repository/v1/datasets/" + summaryModel.getId() + "/files")
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
    MvcResult result3 =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
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
    assertEquals(
        "file description 1 correct", fileLoadModel1.getDescription(), fileModel1.getDescription());

    // check the response from the second ingest request
    MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
    FileModel fileModel2 = connectedOperations.handleSuccessCase(response2, FileModel.class);
    assertEquals(
        "file description 2 correct", fileLoadModel2.getDescription(), fileModel2.getDescription());

    // check the response from the delete request, confirm fails with a lock exception
    MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
    ErrorModel errorModel3 =
        connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        "delete failed on lock exception",
        errorModel3.getMessage(),
        startsWith("Failed to lock the dataset"));

    // delete the dataset again and check that it succeeds now that there are no outstanding locks
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testSharedLockFileDelete() throws Exception {
    // Make sure that dataset delete fails on lock conflict
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_SKIP_RETRY_FAULT.name(), true);
    // ingest two files
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetPath1 = "/mm/" + Names.randomizeName("testdir") + "/testfile1.txt";
    FileLoadModel fileLoadModel1 =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 1")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
    FileModel fileModel1 =
        connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel1);

    String targetPath2 = "/mm/" + Names.randomizeName("testdir") + "/testfile2.txt";
    FileLoadModel fileLoadModel2 =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("file 2")
            .mimeType("text/plain")
            .targetPath(targetPath2)
            .profileId(billingProfile.getId());
    FileModel fileModel2 =
        connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel2);

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteFileLookupStep
    configService.setFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // try to delete the first file
    MvcResult result1 =
        mvc.perform(
                delete(
                    "/api/repository/v1/datasets/"
                        + summaryModel.getId()
                        + "/files/"
                        + fileModel1.getFileId()))
            .andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has a shared lock
    // note: asserts are below outside the hang block
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

    // try to delete the second file
    MvcResult result2 =
        mvc.perform(
                delete(
                    "/api/repository/v1/datasets/"
                        + summaryModel.getId()
                        + "/files/"
                        + fileModel2.getFileId()))
            .andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has two shared locks
    // note: asserts are below outside the hang block
    String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
    String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

    // try to delete the dataset, this should fail with a lock exception
    // note: asserts are below outside the hang block
    MvcResult result3 =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
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

    // check that the delete request launched while the dataset had shared locks on it, failed with
    // a lock exception
    MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
    ErrorModel errorModel3 =
        connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        "delete failed on lock exception",
        errorModel3.getMessage(),
        startsWith("Failed to lock the dataset"));

    // delete the dataset again and check that it succeeds now that there are no outstanding locks
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testSharedLockTableIngest() throws Exception {
    // Make sure that dataset delete fails on lock conflict
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_SKIP_RETRY_FAULT.name(), true);

    // load a JSON file that contains the table rows to load into the test bucket
    String resourceFileName = "snapshot-test-dataset-data-without-rowids.json";
    String dirInCloud = "scratch/testSharedLockTableIngest/" + UUID.randomUUID().toString();
    String gsPath = uploadIngestInputFile(resourceFileName, dirInCloud);

    // make sure the JSON file gets cleaned up on test teardown
    connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in IngestSetupStep
    configService.setFault(ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off the first table ingest. it should hang in the cleanup step.
    IngestRequestModel ingestRequest1 =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("thetable")
            .path(gsPath);
    MvcResult result1 = connectedOperations.ingestTableRaw(summaryModel.getId(), ingestRequest1);
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has a shared lock
    // note: asserts are below outside the hang block
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

    // kick off the second table ingest. it should hang in the cleanup step.
    IngestRequestModel ingestRequest2 =
        new IngestRequestModel()
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
    MvcResult result3 =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
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
    ErrorModel errorModel3 =
        connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        "delete failed on lock exception",
        errorModel3.getMessage(),
        startsWith("Failed to lock the dataset"));

    // delete the dataset again and check that it succeeds now that there are no outstanding locks
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  // ------ Retry exclusive lock/unlock tests ---------------

  @Test
  public void retryAndAcquireExclusiveLock() throws Exception {
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    assertNull("At beginning of test, dataset should have no exclusive lock", exclusiveLock1);

    configService.setFault(ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT.toString(), true);

    MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();
    logger.info(
        "Sleeping for 2 seconds during delete dataset flight. It should fail to acquire exclusive lock.");
    TimeUnit.SECONDS.sleep(2);
    String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
    assertNull("Exclusive lock should be null while fault is set", exclusiveLock2);

    configService.setFault(ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT.toString(), false);
    logger.info("Fault removed - delete dataset flight should now succeed.");
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    // if successful, remove dataset id from tracking methods
    // so that cleanup does not try to remove the dataset again
    HttpStatus status = HttpStatus.valueOf(response.getStatus());
    assertTrue(
        "Dataset delete should have successfully completed after acquiring exclusive lock",
        status.is2xxSuccessful());
    if (connectedOperations.checkDeleteResponse(response)) {
      connectedOperations.removeDatasetFromTracking(datasetId);
    }
    logger.info("Dataset successfully deleted after acquiring exclusive lock.");
  }

  @Test
  public void retryAndFailAcquireExclusiveLock() throws Exception {
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    assertNull("At beginning of test, dataset should have no exclusive lock", exclusiveLock1);

    configService.setFault(ConfigEnum.FILE_INGEST_LOCK_FATAL_FAULT.toString(), true);

    MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();
    logger.info("Sleeping for 5 seconds while delete dataset attempts to delete. It should fail.");
    TimeUnit.SECONDS.sleep(5);
    String exclusiveLock2 = datasetDao.getExclusiveLock(datasetId);
    assertNull("Exclusive lock should be null while fault is set", exclusiveLock2);

    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    connectedOperations.handleFailureCase(response);
  }

  @Test
  public void retryAndAcquireExclusiveUnlock() throws Exception {
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    assertNull("At beginning of test, dataset should have no exclusive lock", exclusiveLock1);

    configService.setFault(ConfigEnum.FILE_INGEST_UNLOCK_RETRY_FAULT.toString(), true);

    MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();
    logger.info(
        "Sleeping for 10 seconds during delete dataset flight. It should acquire exclusive lock.");
    TimeUnit.SECONDS.sleep(10);

    configService.setFault(ConfigEnum.FILE_INGEST_UNLOCK_RETRY_FAULT.toString(), false);
    logger.info("Fault removed - delete dataset flight should now succeed.");
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    // if successful, remove dataset id from tracking methods
    // so that cleanup does not try to remove the dataset again
    HttpStatus status = HttpStatus.valueOf(response.getStatus());
    assertTrue(
        "Dataset delete should have successfully completed after acquiring exclusive lock",
        status.is2xxSuccessful());
    if (connectedOperations.checkDeleteResponse(response)) {
      connectedOperations.removeDatasetFromTracking(datasetId);
    }
    logger.info("Dataset successfully deleted after acquiring exclusive lock.");
  }

  @Test
  public void retryAndFailAcquireExclusiveUnlock() throws Exception {
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = datasetDao.getExclusiveLock(datasetId);
    assertNull("At beginning of test, dataset should have no exclusive lock", exclusiveLock1);

    configService.setFault(ConfigEnum.FILE_INGEST_UNLOCK_FATAL_FAULT.toString(), true);

    MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();

    logger.info("Fatal fault. Dataset delete task should not succeed.");
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    connectedOperations.handleFailureCase(response);
    logger.info("Dataset successfully deleted, but failed to release lock, so task fails.");

    // Let's handle the dataset cleanup here.
    // (if test fails before this, it's ok - regular teardown should handle it)

    // Dataset delete technically succeeds before the unlock fails, even though the task fails
    // so it will error in teardown if we try to remove the dataset.
    // In case something went wrong, let's try to delete the dataset again

    configService.setFault(ConfigEnum.FILE_INGEST_UNLOCK_FATAL_FAULT.toString(), false);
    MvcResult cleanupResult =
        mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();
    MockHttpServletResponse cleanupResponse =
        connectedOperations.validateJobModelAndWait(cleanupResult);
    HttpStatus status = HttpStatus.valueOf(cleanupResponse.getStatus());

    // worst case: Test fails, but dataset is still cleaned up
    assertFalse(
        "If everything went as expected, delete should have already happened.",
        status.is2xxSuccessful());

    // since we just deleted the dataset, remove it from the cleanup tasks
    connectedOperations.removeDatasetFromTracking(datasetId);
  }

  @Test
  public void testExcludeLockedFromDatasetLookups() throws Exception {
    // check that the dataset metadata row is unlocked
    UUID datasetId = summaryModel.getId();
    String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("dataset row is not exclusively locked", exclusiveLock);
    String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
    assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

    // retrieve the dataset and check that it finds it
    DatasetModel datasetModel = connectedOperations.getDataset(summaryModel.getId());
    assertEquals(
        "Lookup unlocked dataset succeeds", summaryModel.getName(), datasetModel.getName());

    // enumerate datasets and check that this dataset is included in the set
    EnumerateDatasetModel enumerateDatasetModel =
        connectedOperations.enumerateDatasets(summaryModel.getName());
    List<DatasetSummaryModel> enumeratedDatasets = enumerateDatasetModel.getItems();
    boolean foundDatasetWithMatchingId = false;
    for (DatasetSummaryModel enumeratedDataset : enumeratedDatasets) {
      if (enumeratedDataset.getId().equals(summaryModel.getId())) {
        foundDatasetWithMatchingId = true;
        break;
      }
    }
    assertTrue("Unlocked included in enumeration", foundDatasetWithMatchingId);

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteDatasetPrimaryDataStep
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off a request to delete the dataset. this should hang before unlocking the dataset
    // object.
    MvcResult deleteResult =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has an exclusive lock
    // note: asserts are below outside the hang block
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    sharedLocks = datasetDao.getSharedLocks(datasetId);

    // retrieve the dataset, should return not found
    // note: asserts are below outside the hang block
    MvcResult retrieveResult =
        mvc.perform(get("/api/repository/v1/datasets/" + datasetId)).andReturn();

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

    // check that the enumerate request returned successfully and that this dataset is not included
    // in the set
    enumerateDatasetModel =
        connectedOperations.handleSuccessCase(
            enumerateResult.getResponse(), EnumerateDatasetModel.class);
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
    MockHttpServletResponse deleteResponse =
        connectedOperations.validateJobModelAndWait(deleteResult);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
    assertEquals(
        "Dataset delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testExcludeLockedFromFileLookups() throws Exception {
    // check that the dataset metadata row is unlocked
    UUID datasetId = summaryModel.getId();
    String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("dataset row is not exclusively locked", exclusiveLock);
    String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
    assertEquals("dataset row has no shared lock", 0, sharedLocks.length);

    // ingest a file
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetPath1 =
        "/mm/" + Names.randomizeName("testdir") + "/testExcludeLockedFromFileLookups.txt";
    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("testExcludeLockedFromFileLookups")
            .mimeType("text/plain")
            .targetPath(targetPath1)
            .profileId(billingProfile.getId());
    FileModel fileModel =
        connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel);

    // lookup the file by id and check that it's found
    FileModel fileModelFromIdLookup =
        connectedOperations.lookupFileSuccess(summaryModel.getId(), fileModel.getFileId());
    assertEquals(
        "File found by id lookup",
        fileModel.getDescription(),
        fileModelFromIdLookup.getDescription());

    // lookup the file by path and check that it's found
    FileModel fileModelFromPathLookup =
        connectedOperations.lookupFileByPathSuccess(summaryModel.getId(), fileModel.getPath(), -1);
    assertEquals(
        "File found by path lookup",
        fileModel.getDescription(),
        fileModelFromPathLookup.getDescription());

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteDatasetPrimaryDataStep
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off a request to delete the dataset. this should hang before unlocking the dataset
    // object.
    MvcResult deleteResult =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
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
    assertEquals(
        "File NOT found by id lookup",
        HttpStatus.NOT_FOUND,
        HttpStatus.valueOf(lookupFileByIdResponse.getStatus()));

    // check that the lookup file by path returned not found
    assertEquals(
        "File NOT found by path lookup",
        HttpStatus.NOT_FOUND,
        HttpStatus.valueOf(lookupFileByPathResponse.getStatus()));

    // check the response from the delete request
    MockHttpServletResponse deleteResponse =
        connectedOperations.validateJobModelAndWait(deleteResult);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
    assertEquals(
        "Dataset delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // remove the file from the connectedoperation bookkeeping list
    connectedOperations.removeFile(summaryModel.getId(), fileModel.getFileId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  private String uploadIngestInputFile(String resourceFileName, String dirInCloud)
      throws IOException {
    BlobInfo ingestTableBlob =
        BlobInfo.newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
    Storage storage = StorageOptions.getDefaultInstance().getService();
    storage.create(
        ingestTableBlob,
        IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
    return "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;
  }
}
