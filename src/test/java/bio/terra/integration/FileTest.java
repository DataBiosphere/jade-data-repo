package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.FSObjectModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.After;
import org.junit.Before;
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
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class FileTest extends UsersBase {

    private static Logger logger = LoggerFactory.getLogger(StudyTest.class);

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private TestConfiguration testConfiguration;

    @Autowired
    private Storage storage;

    private StudySummaryModel studySummaryModel;
    private String studyId;

    @Before
    public void setup() throws Exception {
        super.setup();
        studySummaryModel = dataRepoFixtures.createStudy(steward(), "file-acl-test-study.json");
        studyId = studySummaryModel.getId();
        logger.info("created study " + studyId);
        dataRepoFixtures.addStudyPolicyMember(
            steward(), studySummaryModel.getId(), SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
    }

    @After
    public void tearDown() throws Exception {
        if (studyId != null) {
            dataRepoFixtures.deleteStudy(steward(), studyId);
        }
    }

    @Test
    public void fileUnauthorizedPermissionsTest() throws Exception {

        String gsPath = "gs://" + testConfiguration.getIngestbucket();
        String filePath = "/foo/bar";

        DataRepoResponse<JobModel> launchResp = dataRepoFixtures.ingestFileLaunch(
            custodian(), studyId, gsPath + "/files/File%20Design%20Notes.pdf", filePath);
        assertThat("Custodian is not authorized to ingest a file",
            launchResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        FSObjectModel fsObjectModel = dataRepoFixtures.ingestFile(
            steward(), studyId, gsPath + "/files/File%20Design%20Notes.pdf", filePath);
        String fileId = fsObjectModel.getObjectId();

        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileId);

        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo)) {
            writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
        }

        IngestResponseModel ingestResponseModel = dataRepoFixtures.ingestJsonData(
            steward(), studyId, "file", targetPath);

        assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

        dataRepoFixtures.getFileById(steward(), studyId, fileId);
        dataRepoFixtures.getFileById(custodian(), studyId, fileId);

        DataRepoResponse<FSObjectModel> getResp = dataRepoFixtures.getFileByIdRaw(reader(), studyId, fileId);
        assertThat("Reader is not authorized to get a file from a study",
            getResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // get file by id
        DataRepoResponse<FSObjectModel> fileResp = dataRepoFixtures.getFileByIdRaw(discoverer(), studyId, fileId);
        assertThat("Discoverer is not authorized to get a file from a study",
            fileResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        // get file by name
        dataRepoFixtures.getFileByName(steward(), studyId, filePath);
        dataRepoFixtures.getFileByName(custodian(), studyId, filePath);
        DataRepoResponse<FSObjectModel> getResp2 = dataRepoFixtures.getFileByNameRaw(reader(), studyId, filePath);
        assertThat("Reader is not authorized to get a file from a study",
            getResp2.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<FSObjectModel> fileResp2 =
            dataRepoFixtures.getFileByNameRaw(discoverer(), studyId, filePath);
        assertThat("Discoverer is not authorized to get file",
            fileResp2.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));


        // delete
        DataRepoResponse<JobModel> job = dataRepoFixtures.deleteFileLaunch(reader(), studyId, fileId);
        assertThat("Reader is not authorized to delete file",
            job.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        job = dataRepoFixtures.deleteFileLaunch(custodian(), studyId, fileId);
        assertThat("Custodian is not authorized to delete file",
            job.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        dataRepoFixtures.deleteFile(steward(), studyId, fileId);
    }
}
