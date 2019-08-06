package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.pdao.gcs.GcsProject;
import bio.terra.service.SamClientService;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import java.util.UUID;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
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

    private static final int samTimeout = 300;
    private static final Pattern drsIdRegex = Pattern.compile("([^/]+)$");

    private String discovererToken;
    private String readerToken;
    private String custodianToken;
    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private static final int samTimeoutSeconds = 60 * 10;

    @Before
    public void setup() throws Exception {
        super.setup();
        discovererToken = authService.getDirectAccessAuthToken(discoverer().getEmail());
        readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
        custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
    }

    private Storage getStorage(String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        StorageOptions storageOptions = StorageOptions.newBuilder()
            .setCredentials(googleCredentials)
            .build();
        return storageOptions.getService();
    }

    private GcsProject getGcsProject(String projectId, String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        return new GcsProject(projectId, googleCredentials);
    }

    @Test
    public void checkShared() throws  Exception {
        dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "participant", "ingest-test/ingest-test-participant.json");

        dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "sample", "ingest-test/ingest-test-sample.json");

        DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
        String datasetBqSnapshotName = "datarepo_" + dataset.getName();

        BigQuery custodianBigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), custodianToken);
        try {
            BigQueryFixtures.datasetExists(custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);
            fail("custodian shouldn't be able to access bq dataset before it is shared with them");
        } catch (IllegalStateException e) {
            assertThat("checking message for pdao exception error",
                e.getMessage(),
                equalTo("existence check failed for " + datasetBqSnapshotName));
        }

        dataRepoFixtures.addDatasetPolicyMember(
            steward(),
            datasetId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian().getEmail());
        DataRepoResponse<EnumerateDatasetModel> enumDatasets = dataRepoFixtures.enumerateDatasetsRaw(custodian());
        assertThat("Custodian is authorized to enumerate datasets",
            enumDatasets.getStatusCode(),
            equalTo(HttpStatus.OK));

        boolean custodianHasAccess = TestUtils.eventualExpect(5, samTimeoutSeconds, true, () -> {
            try {
                boolean bqDatasetExists = BigQueryFixtures.datasetExists(
                    custodianBigQuery,
                    dataset.getDataProject(),
                    datasetBqSnapshotName);
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

        assertThat("custodian can access the bq snapshot after it has been shared",
            custodianHasAccess,
            equalTo(true));

        SnapshotSummaryModel snapshotSummaryModel =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-snapshot.json");

        DatasetModel snapshotModel = dataRepoFixtures.getDataset(custodian(), datasetSummaryModel.getId());
        BigQuery bigQuery = BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), readerToken);
        try {
            BigQueryFixtures.datasetExists(bigQuery, snapshotModel.getDataProject(), snapshotModel.getName());
            fail("reader shouldn't be able to access bq dataset before it is shared with them");
        } catch (IllegalStateException e) {
            assertThat("checking message for exception error",
                 e.getMessage(),
                 equalTo("existence check failed for ".concat(snapshotSummaryModel.getName())));
        }

        dataRepoFixtures.addSnapshotPolicyMember(
            custodian(),
            snapshotSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader().getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader().getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASNAPSHOT,
            snapshotSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        boolean readerHasAccess = TestUtils.eventualExpect(5, samTimeoutSeconds, true, () -> {
            try {
                boolean snapshotExists = BigQueryFixtures.datasetExists(bigQuery,
                    snapshotModel.getDataProject(),
                    snapshotSummaryModel.getName());
                assertTrue("snapshot exists and is accessible", snapshotExists);
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "access is denied until SAM syncs the reader policy with Google",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });

        assertThat("reader can access the snapshot after it has been shared",
            readerHasAccess,
            equalTo(true));
    }

    @Test
    public void fileAclTest() throws Exception {
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "file-acl-test-dataset.json");
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetSummaryModel.getId(), SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
        DatasetModel datasetModel = dataRepoFixtures.getDataset(steward(), datasetSummaryModel.getId());

        // Step 1. Ingest a file into the study
        String gsPath = "gs://" + testConfiguration.getIngestbucket();
        FSObjectModel fsObjectModel = dataRepoFixtures.ingestFile(
            steward(),
            datasetSummaryModel.getId(),
            gsPath + "/files/File%20Design%20Notes.pdf",
            "/foo/bar");

        // Step 2. Ingest one row into the study 'file' table with a reference to that ingested file
        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fsObjectModel.getObjectId());
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        Storage storage = StorageOptions.getDefaultInstance().getService();
        try (WriteChannel writer = storage.writer(targetBlobInfo)) {
            writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
        }

        IngestResponseModel ingestResponseModel = dataRepoFixtures.ingestJsonData(
            steward(),
            datasetSummaryModel.getId(),
            "file",
            targetPath);

        assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

        // Step 3. Create a snapshot exposing the one row and grant read access to our reader.
        SnapshotSummaryModel snapshotSummaryModel = dataRepoFixtures.createSnapshot(
            custodian(),
            datasetSummaryModel,
            "file-acl-test-snapshot.json");

        SnapshotModel snapshotModel = dataRepoFixtures.getSnapshot(custodian(), snapshotSummaryModel.getId());

        dataRepoFixtures.addSnapshotPolicyMember(
            custodian(),
            snapshotModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader().getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader().getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASNAPSHOT,
            snapshotModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        // Step 4. Wait for SAM to sync the access change out to GCP.
        //
        // We make a BigQuery context for the reader in the test project. The reader doesn't have access
        // to run queries in the dataset project.
        BigQuery bigQueryReader = BigQueryFixtures.getBigQuery(testConfiguration.getGoogleProjectId(), readerToken);

        TestUtils.eventualExpect(5, samTimeout, true, () -> {
            try {
                boolean snapshotExists = BigQueryFixtures.datasetExists(
                    bigQueryReader,
                    datasetModel.getDataProject(),
                    snapshotModel.getName());

                assertThat("Snapshot wasn't created right", snapshotExists, equalTo(true));
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "checking message for exception error",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });

        // Step 5. Read and validate the DRS URI from the file ref column in the 'file' table.
        String drsObjectId = BigQueryFixtures.queryForDrsId(bigQueryReader,
            snapshotModel,
            "file",
            "file_ref");

        // Step 6. Use DRS API to lookup the file by DRS ID (pulled out of the URI).
        DRSObject drsObject = dataRepoFixtures.drsGetObject(reader(), drsObjectId);
        List<DRSAccessMethod> accessMethods = drsObject.getAccessMethods();
        assertThat("access method is not null and length 1", accessMethods.size(), equalTo(1));

        // Step 7. Pull our the gs path try to read the file as reader and discoverer
        DRSAccessURL accessUrl = accessMethods.get(0).getAccessUrl();

        String[] strings = accessUrl.getUrl().split("/", 4);

        String bucketName = strings[2];
        String blobName = strings[3];
        BlobId blobId = BlobId.of(bucketName, blobName);

        Storage readerStorage = getStorage(readerToken);
        assertTrue("Reader can read some bytes of the file", canReadBlob(readerStorage, blobId));

        Storage discovererStorage = getStorage(discovererToken);
        assertFalse("Discoverer can not read the file", canReadBlob(discovererStorage, blobId));
    }

    private boolean canReadBlob(Storage storage, BlobId blobId) {
        try (ReadChannel reader = storage.reader(blobId)) {
            ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
            int bytesRead = reader.read(bytes);
            return (bytesRead > 0);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            return false;
        }
    }

}
