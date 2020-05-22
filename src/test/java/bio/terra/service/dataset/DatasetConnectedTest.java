package bio.terra.service.dataset;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
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
    @Autowired private DatasetDao datasetDao;
    @Autowired private ConfigurationService configService;
    @Autowired private ConnectedTestConfiguration testConfig;
    @MockBean private IamProviderInterface samService;

    private BillingProfileModel billingProfile;

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
    public void testDuplicateName() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel datasetSummaryModel = connectedOperations.createDataset(datasetRequest);
        assertNotNull("created dataset successfully the first time", datasetSummaryModel);
        connectedOperations.addDataset(datasetSummaryModel.getId());

        // fetch the dataset and confirm the metadata matches the request
        DatasetModel datasetModel = connectedOperations.getDataset(datasetSummaryModel.getId());
        assertNotNull("fetched dataset successfully after creation", datasetModel);
        assertEquals("fetched dataset name matches request", datasetRequest.getName(), datasetModel.getName());

        // check that the dataset metadata row is unlocked
        String exclusiveLock = datasetDao.getExclusiveLock(UUID.fromString(datasetSummaryModel.getId()));
        assertNull("dataset row is unlocked", exclusiveLock);

        // try to create the same dataset again and check that it fails
        ErrorModel errorModel =
            connectedOperations.createDatasetExpectError(datasetRequest, HttpStatus.BAD_REQUEST);
        assertThat("error message includes name conflict",
            errorModel.getMessage(), containsString("Dataset name already exists"));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(datasetSummaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(datasetSummaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testOverlappingDeletes() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);
        connectedOperations.addDataset(summaryModel.getId());

        // enable wait in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the dataset
        MvcResult result1 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();

        // try to delete the dataset again
        MvcResult result2 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        ErrorModel errorModel2 = connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel2.getMessage(),
            startsWith("Failed to lock the dataset"));

        // disable wait in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

        // check the response from the first delete request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
        assertEquals("First delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testSharedLockFileIngest() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);
        connectedOperations.addDataset(summaryModel.getId());

        // enable wait in IngestFileIdStep
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
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row has no exclusive lock", exclusiveLock);
        String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks);
        assertEquals("dataset row has one shared lock", 1, sharedLocks.length);

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
        exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row has no exclusive lock", exclusiveLock);
        sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks);
        assertEquals("dataset row has two shared locks", 2, sharedLocks.length);

        // try to delete the dataset, confirm fails with a lock exception
        MvcResult result3 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
        ErrorModel errorModel3 = connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel3.getMessage(),
            startsWith("Failed to lock the dataset"));

        // disable wait in IngestFileIdStep
        configService.setFault(ConfigEnum.FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

        // check the response from the first ingest request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        FileModel fileModel1 = connectedOperations.handleSuccessCase(response1, FileModel.class);
        assertEquals("file description 1 correct", fileLoadModel1.getDescription(), fileModel1.getDescription());

        // check the response from the second ingest request
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        FileModel fileModel2 = connectedOperations.handleSuccessCase(response2, FileModel.class);
        assertEquals("file description 2 correct", fileLoadModel2.getDescription(), fileModel2.getDescription());

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testSharedLockFileDelete() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);

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

        // enable wait in DeleteFileLookupStep
        configService.setFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the first file
        MvcResult result1 = mvc.perform(
            delete("/api/repository/v1/datasets/" + summaryModel.getId() + "/files/" + fileModel1.getFileId()))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has a shared lock
        UUID datasetId = UUID.fromString(summaryModel.getId());
        String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row has no exclusive lock", exclusiveLock);
        String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks);
        assertEquals("dataset row has one shared lock", 1, sharedLocks.length);

        // try to delete the second file
        MvcResult result2 = mvc.perform(
            delete("/api/repository/v1/datasets/" + summaryModel.getId() + "/files/" + fileModel2.getFileId()))
            .andReturn();
        TimeUnit.SECONDS.sleep(5); // give the flight time to launch

        // check that the dataset metadata row has two shared locks
        exclusiveLock = datasetDao.getExclusiveLock(datasetId);
        assertNull("dataset row has no exclusive lock", exclusiveLock);
        sharedLocks = datasetDao.getSharedLocks(datasetId);
        assertNotNull("dataset row has a shared lock taken out", sharedLocks);
        assertEquals("dataset row has two shared locks", 2, sharedLocks.length);

        // try to delete the dataset, confirm fails with a lock exception
        MvcResult result3 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        MockHttpServletResponse response3 = connectedOperations.validateJobModelAndWait(result3);
        ErrorModel errorModel3 = connectedOperations.handleFailureCase(response3, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel3.getMessage(),
            startsWith("Failed to lock the dataset"));

        // disable wait in DeleteFileLookupStep
        configService.setFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

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

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(summaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }
}
