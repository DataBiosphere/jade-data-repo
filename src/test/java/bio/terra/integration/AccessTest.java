package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.gcs.GcsProject;
import bio.terra.service.SamClientService;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class AccessTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(AccessTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private AuthService authService;
    @Autowired private SamClientService samClientService;
    @Autowired private TestConfiguration testConfiguration;
    @Autowired private Storage storage;

    private static final int samTimeout = 300;
    private static final Pattern drsIdRegex = Pattern.compile("([^/]+)$");

    private String readerToken;
    private String custodianToken;
    private StudySummaryModel studySummaryModel;
    private String studyId;
    private static final int samTimeoutSeconds = 60 * 10;

    @Before
    public void setup() throws Exception {
        super.setup();
        readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
        custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
        studySummaryModel = dataRepoFixtures.createStudy(steward(), "ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }

    private BigQuery getBigQuery(String projectId, String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        return BigQueryFixtures.getBigQuery(projectId, googleCredentials);
    }

    private GcsProject getGcsProject(String projectId, String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        return new GcsProject(projectId, googleCredentials);
    }

    @Test
    public void checkShared() throws  Exception {
        dataRepoFixtures.ingestJsonData(
            steward(), studyId, "participant", "ingest-test/ingest-test-participant.json");

        dataRepoFixtures.ingestJsonData(
            steward(), studyId, "sample", "ingest-test/ingest-test-sample.json");

        StudyModel study = dataRepoFixtures.getStudy(steward(), studyId);
        String studyBqDatasetName = "datarepo_" + study.getName();

        BigQuery custodianBigQuery = getBigQuery(study.getDataProject(), custodianToken);
        try {
            BigQueryFixtures.datasetExists(custodianBigQuery, study.getDataProject(), studyBqDatasetName);
            fail("custodian shouldn't be able to access bq dataset before it is shared with them");
        } catch (IllegalStateException e) {
            assertThat("checking message for pdao exception error",
                e.getMessage(),
                equalTo("existence check failed for " + studyBqDatasetName));
        }

        dataRepoFixtures.addStudyPolicyMember(
            steward(),
            studyId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian().getEmail());
        DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodian());
        assertThat("Custodian is authorized to enumerate studies",
            enumStudies.getStatusCode(),
            equalTo(HttpStatus.OK));

        boolean custodianHasAccess = TestUtils.eventualExpect(5, samTimeoutSeconds, true, () -> {
            try {
                boolean bqDatasetExists = BigQueryFixtures.datasetExists(
                    custodianBigQuery,
                    study.getDataProject(),
                    studyBqDatasetName);
                assertThat("study bq dataset exists and is accessible", bqDatasetExists, equalTo(true));
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "access is denied until SAM syncs the custodian policy with Google",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });

        assertThat("custodian can access the bq dataset after it has been shared",
            custodianHasAccess,
            equalTo(true));

        DatasetSummaryModel datasetSummaryModel =
            dataRepoFixtures.createDataset(custodian(), studySummaryModel, "ingest-test-dataset.json");

        DatasetModel datasetModel = dataRepoFixtures.getDataset(custodian(), datasetSummaryModel.getId());
        BigQuery bigQuery = getBigQuery(datasetModel.getDataProject(), readerToken);
        try {
            BigQueryFixtures.datasetExists(bigQuery, datasetModel.getDataProject(), datasetSummaryModel.getName());
            fail("reader shouldn't be able to access bq dataset before it is shared with them");
        } catch (IllegalStateException e) {
            assertThat("checking message for exception error",
                 e.getMessage(),
                 equalTo("existence check failed for ".concat(datasetSummaryModel.getName())));
        }

        dataRepoFixtures.addDatasetPolicyMember(
            custodian(),
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader().getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader().getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        boolean readerHasAccess = TestUtils.eventualExpect(5, samTimeoutSeconds, true, () -> {
            try {
                boolean datasetExists = BigQueryFixtures.datasetExists(bigQuery,
                    datasetModel.getDataProject(),
                    datasetSummaryModel.getName());
                assertTrue("dataset exists and is accessible", datasetExists);
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "access is denied until SAM syncs the reader policy with Google",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });

        assertThat("reader can access the dataset after it has been shared",
            readerHasAccess,
            equalTo(true));
    }

    @Test
    public void fileAclTest() throws Exception {
        studySummaryModel = dataRepoFixtures.createStudy(steward(), "file-acl-test-study.json");
        dataRepoFixtures.addStudyPolicyMember(
            steward(), studySummaryModel.getId(), SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
        StudyModel studyModel = dataRepoFixtures.getStudy(steward(), studySummaryModel.getId());
        BigQuery bigQuery = getBigQuery(studyModel.getDataProject(), readerToken);

        String gsPath = "gs://" + testConfiguration.getIngestbucket();

        FSObjectModel fsObjectModel = dataRepoFixtures.ingestFile(
            steward(),
            studySummaryModel.getId(),
            gsPath + "/files/File%20Design%20Notes.pdf",
            "/foo/bar");

        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fsObjectModel.getObjectId());

        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo)) {
            writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
        }

        IngestResponseModel ingestResponseModel = dataRepoFixtures.ingestJsonData(
            steward(),
            studySummaryModel.getId(),
            "file",
            targetPath);

        assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

        DatasetSummaryModel datasetSummaryModel = dataRepoFixtures.createDataset(
            custodian(),
            studySummaryModel,
            "file-acl-test-dataset.json");

        dataRepoFixtures.addDatasetPolicyMember(
            custodian(),
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader().getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader().getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        TestUtils.eventualExpect(5, samTimeout, true, () -> {
            try {
                boolean datasetExists = BigQueryFixtures.datasetExists(
                    bigQuery,
                    studyModel.getDataProject(),
                    datasetSummaryModel.getName());

                assertThat("Dataset wasn't created right", datasetExists, equalTo(true));
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "checking message for pdao exception error",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });

        BigQuery bigQueryReader = getBigQuery(testConfiguration.getGoogleProjectId(), readerToken);
        String sql = String.format("SELECT file_ref FROM `%s.%s.file`",
            testConfiguration.getGoogleProjectId(), datasetSummaryModel.getName());

        TableResult ids = BigQueryFixtures.query(sql, bigQueryReader);

        String drsId = null;
        for (FieldValueList fieldValueList : ids.iterateAll()) {
            drsId = fieldValueList.get(0).getStringValue();
        }

        assertThat("drs id was found", drsId, notNullValue());
        Matcher matcher = drsIdRegex.matcher(drsId);

        assertThat("matcher found a match in the drs id", matcher.find(), equalTo(true));
        drsId = matcher.group();

        Optional<DRSObject> optionalDRSObject = dataRepoFixtures.resolveDrsId(
            reader(),
            drsId).getResponseObject();

        assertThat("there is a response", optionalDRSObject.isPresent(), equalTo(true));

        List<DRSAccessMethod> accessMethods = optionalDRSObject.get().getAccessMethods();

        assertThat("access method is not null and length 1", accessMethods.size(), equalTo(1));

        DRSAccessURL accessUrl = accessMethods.get(0).getAccessUrl();

        String[] strings = accessUrl.getUrl().split("/", 4);

        String bucketName = strings[2];
        String blobName = strings[3];

        try (ReadChannel reader = storage.reader(BlobId.of(bucketName, blobName))) {
            ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
            assertThat("Reader can read some bytes of the pdf", reader.read(bytes), greaterThan(0));
        }

    }

}
