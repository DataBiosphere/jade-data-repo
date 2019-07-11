package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.filesystem.EncodeFileIn;
import bio.terra.filesystem.EncodeFileOut;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.*;
import bio.terra.pdao.bigquery.BigQueryConfiguration;
import bio.terra.pdao.bigquery.BigQueryProject;
import bio.terra.pdao.exception.PdaoException;
import bio.terra.pdao.gcs.GcsProject;
import bio.terra.service.SamClientService;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
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

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class AccessTest {
    private static final String omopStudyName = "it_study_omop";
    private static final String omopStudyDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";
    private static Logger logger = LoggerFactory.getLogger(StudyTest.class);

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private Users users;

    @Autowired
    private AuthService authService;

    @Autowired
    private SamClientService samClientService;

    @Autowired
    private BigQueryConfiguration bigQueryConfiguration;

    @Autowired
    private TestConfiguration testConfiguration;

    @Autowired
    private Storage storage;

    private TestConfiguration.User steward;
    private TestConfiguration.User custodian;
    private TestConfiguration.User reader;
    private static final int samTimeout = 300000;
    private String stewardToken;
    private String custodianToken;
    private String readerToken;
    private StudySummaryModel studySummaryModel;
    private String studyId;
    private static final int samTimeout = 300000;


    @Before
    public void setup() throws Exception {
        steward = users.getUserForRole("steward");
        custodian = users.getUser("harry");
        reader = users.getUserForRole("reader");
        readerToken = authService.getDirectAccessAuthToken(reader.getEmail());

        studySummaryModel = dataRepoFixtures.createStudy(steward, "ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }

    private BigQueryProject getBigQueryProject(String token) {
        String projectId = bigQueryConfiguration.googleProjectId();
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        BigQueryProject bigQueryProject = new BigQueryProject(projectId, googleCredentials);

        return bigQueryProject;
    }

    private GcsProject getGcsProject(String token) {
        String projectId = bigQueryConfiguration.googleProjectId();
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        GcsProject gcsProject = new GcsProject(projectId, googleCredentials);

        return gcsProject;
    }

    @Test
    public void checkShared() throws  Exception {
        BigQueryProject bigQueryProject = getBigQueryProject(readerToken);

        dataRepoFixtures.ingestJsonData(
            steward, studyId, "participant", "ingest-test/ingest-test-participant.json");

        dataRepoFixtures.ingestJsonData(
            steward, studyId, "sample", "ingest-test/ingest-test-sample.json");

        dataRepoFixtures.addStudyPolicyMember(
            steward,
            studyId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian.getEmail());
        DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodian);
        assertThat("Custodian is authorized to enumerate studies",
            enumStudies.getStatusCode(),
            equalTo(HttpStatus.OK));

        DatasetSummaryModel datasetSummaryModel =
            dataRepoFixtures.createDataset(custodian, studySummaryModel, "ingest-test-dataset.json");

        try {
            bigQueryProject.datasetExists(datasetSummaryModel.getName());
            assertThat("reader can access the dataset after it has been shared",
                bigQueryProject.datasetExists(datasetSummaryModel.getName()),
                not(true));
        } catch (PdaoException e) {
            assertThat("checking message for pdao exception error",
                 e.getMessage(),
                 equalTo("existence check failed for ".concat(datasetSummaryModel.getName())));
        }

        dataRepoFixtures.addDatasetPolicyMember(
            custodian,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader.getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));


        long startTime = System.currentTimeMillis();
        boolean hasAccess = false;
        while (!hasAccess && (System.currentTimeMillis() - startTime) < samTimeout) {
            TimeUnit.SECONDS.sleep(5);
            try {
                boolean datasetExists = bigQueryProject.datasetExists(datasetSummaryModel.getName());
                hasAccess = true;
                assertThat("Dataset wasn't created right", datasetExists, equalTo(true));
            } catch (PdaoException e) {
                assertThat(
                    "checking message for pdao exception error",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
            }
        }

        assertThat("reader can access the dataset after it has been shared",
            hasAccess,
            equalTo(true));
    }

    @Test
    public void fileAclTest() throws Exception{
        GcsProject gcsProject = getGcsProject(readerToken);

        studySummaryModel = dataRepoFixtures.createStudy(steward, "file-acl-test-study.json");
        String gsPath = "gs://" + testConfiguration.getIngestbucket();

        FSObjectModel fsObjectModel = dataRepoFixtures.ingestFile(
            steward,
            studySummaryModel.getId(),
            gsPath + "/files/File%20Design%20Notes.pdf",
            "/foo/bar");

        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fsObjectModel.getObjectId());

        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo)) {
            writer.write(ByteBuffer.wrap(json.getBytes("UTF-8")));
        }

        IngestResponseModel ingestResponseModel = dataRepoFixtures.ingestJsonData(
            steward,
            studySummaryModel.getId(),
            "file",
            targetPath);

        assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

        DatasetSummaryModel datasetSummaryModel = dataRepoFixtures.createDataset(custodian, studySummaryModel, "file-acl-test-dataset.json");

        dataRepoFixtures.addDatasetPolicyMember(
            custodian,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader.getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));


        long startTime = System.currentTimeMillis();
        boolean hasAccess = false;
        while (!hasAccess && (System.currentTimeMillis() - startTime) < samTimeout) {
            TimeUnit.SECONDS.sleep(5);
            try {
                gcsProject.getStorage().get(BlobId.of(buck))
                boolean datasetExists = bigQueryProject.datasetExists(datasetSummaryModel.getName());

                hasAccess = true;
                assertThat("Dataset wasn't created right", datasetExists, equalTo(true));
            } catch (PdaoException e) {
        }

        assertThat("reader can access the dataset after it has been shared",
            hasAccess,
            equalTo(true));


    }

}
