package bio.terra.integration;

import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.service.dataset.DatasetTest;
import bio.terra.service.iam.SamClientService;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class FileTest extends UsersBase {

    private static Logger logger = LoggerFactory.getLogger(DatasetTest.class);

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfiguration;

    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private String profileId;

    @Before
    public void setup() throws Exception {
        super.setup();
        profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "file-acl-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        logger.info("created dataset " + datasetId);
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetSummaryModel.getId(), SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
    }

    @After
    public void tearDown() throws Exception {
        if (datasetId != null) {
            dataRepoFixtures.deleteDataset(steward(), datasetId);
        }
    }

    // DR-612 filesystem corruption test; use a non-existent file to make sure everything errors
    // Do file ingests in parallel using a filename that will cause failure
    // TODO: DR-643 needs to be fixed before this test will work reliably.
    //  So for now it has to stay ignored
    @Ignore
    @Test
    public void fileParallelFailedLoadTest() throws Exception {
        List<DataRepoResponse<JobModel>> responseList = new ArrayList<>();
        String gsPath = "gs://" + testConfiguration.getIngestbucket() + "/nonexistentfile";
        String filePath = "/foo" + UUID.randomUUID().toString() + "/bar";

        for (int i = 0; i < 20; i++) {
            DataRepoResponse<JobModel> launchResp = dataRepoFixtures.ingestFileLaunch(
                steward(), datasetId, profileId, gsPath, filePath + i);
            responseList.add(launchResp);
        }

        int failureCount = 0;
        for (DataRepoResponse<JobModel> resp : responseList) {
            DataRepoResponse<FileModel> response = dataRepoClient.waitForResponse(steward(), resp, FileModel.class);
            if (response.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Got expected not found");
            } else {
                System.out.println("Unexpected: " + response.getStatusCode().toString());
                if (response.getErrorObject().isPresent()) {
                    ErrorModel errorModel = response.getErrorObject().get();
                    System.out.println("Error: " + errorModel.getMessage());
                }
                failureCount++;
            }
        }

        assertThat("No unexpected failures", failureCount, equalTo(0));
    }

    @Test
    public void fileUnauthorizedPermissionsTest() throws Exception {

        String gsPath = "gs://" + testConfiguration.getIngestbucket();
        String filePath = "/foo/bar";

        DataRepoResponse<JobModel> launchResp = dataRepoFixtures.ingestFileLaunch(
            custodian(), datasetId, profileId, gsPath + "/files/File%20Design%20Notes.pdf", filePath);
        assertThat("Custodian is not authorized to ingest a file",
            launchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        FileModel fileModel = dataRepoFixtures.ingestFile(
            steward(), datasetId, profileId, gsPath + "/files/File%20Design%20Notes.pdf", filePath);
        String fileId = fileModel.getFileId();

        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileId);

        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        Storage storage = StorageOptions.getDefaultInstance().getService();
        try (WriteChannel writer = storage.writer(targetBlobInfo)) {
            writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
        }

        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "file", targetPath, IngestRequestModel.StrategyEnum.APPEND);
        IngestResponseModel ingestResponseModel = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, request);

        assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

        // validates success
        dataRepoFixtures.getFileById(steward(), datasetId, fileId);
        dataRepoFixtures.getFileById(custodian(), datasetId, fileId);

        DataRepoResponse<FileModel> readerResp = dataRepoFixtures.getFileByIdRaw(reader(), datasetId, fileId);
        assertThat("Reader is not authorized to get a file from a dataset",
            readerResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // get file by id
        DataRepoResponse<FileModel> discovererResp =
            dataRepoFixtures.getFileByIdRaw(discoverer(), datasetId, fileId);
        assertThat("Discoverer is not authorized to get a file from a dataset",
            discovererResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // get file by name validates success
        dataRepoFixtures.getFileByName(steward(), datasetId, filePath);
        dataRepoFixtures.getFileByName(custodian(), datasetId, filePath);

        readerResp = dataRepoFixtures.getFileByNameRaw(reader(), datasetId, filePath);
        assertThat("Reader is not authorized to get a file from a dataset",
            readerResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        discovererResp = dataRepoFixtures.getFileByNameRaw(discoverer(), datasetId, filePath);
        assertThat("Discoverer is not authorized to get file",
            discovererResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // delete
        DataRepoResponse<JobModel> job = dataRepoFixtures.deleteFileLaunch(reader(), datasetId, fileId);
        assertThat("Reader is not authorized to delete file",
            job.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        job = dataRepoFixtures.deleteFileLaunch(custodian(), datasetId, fileId);
        assertThat("Custodian is not authorized to delete file",
            job.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // validates success
        dataRepoFixtures.deleteFile(steward(), datasetId, fileId);
    }
}
