package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.SamClientService;
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
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
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

    private String discovererToken;
    private String readerToken;
    private String custodianToken;
    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private String profileId;

    @Before
    public void setup() throws Exception {
        super.setup();
        discovererToken = authService.getDirectAccessAuthToken(discoverer().getEmail());
        readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
        custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    }

    private Storage getStorage(String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        return GcsFixtures.getStorage(googleCredentials);
    }

    @Test
    public void checkShared() throws  Exception {
        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json", IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

        request = dataRepoFixtures.buildSimpleIngest(
            "sample", "ingest-test/ingest-test-sample.json", IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

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

        boolean custodianHasAccess =
            BigQueryFixtures.hasAccess(custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);

        assertThat("custodian can access the bq snapshot after it has been shared",
            custodianHasAccess,
            equalTo(true));

        SnapshotSummaryModel snapshotSummaryModel =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-snapshot.json");

        SnapshotModel snapshotModel = dataRepoFixtures.getSnapshot(custodian(), snapshotSummaryModel.getId());
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
            new AuthenticatedUserRequest().email(reader().getEmail()).token(Optional.of(readerToken));
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASNAPSHOT,
            snapshotSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        boolean readerHasAccess =
            BigQueryFixtures.hasAccess(bigQuery, snapshotModel.getDataProject(), snapshotModel.getName());
        assertThat("reader can access the snapshot after it has been shared",
            readerHasAccess,
            equalTo(true));
    }

    @Test
    public void fileAclTest() throws Exception {
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "file-acl-test-dataset.json");
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetSummaryModel.getId(), SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());

        // Step 1. Ingest a file into the dataset
        String gsPath = "gs://" + testConfiguration.getIngestbucket();
        FileModel fileModel = dataRepoFixtures.ingestFile(
            steward(),
            datasetSummaryModel.getId(),
            profileId,
            gsPath + "/files/File%20Design%20Notes.pdf",
            "/foo/bar");

        // Step 2. Ingest one row into the study 'file' table with a reference to that ingested file
        String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileModel.getFileId());
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
            steward(), datasetSummaryModel.getId(), request);

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
            new AuthenticatedUserRequest().email(reader().getEmail()).token(Optional.of(readerToken));
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
        BigQueryFixtures.hasAccess(bigQueryReader, snapshotModel.getDataProject(), snapshotModel.getName());

        // Step 5. Read and validate the DRS URI from the file ref column in the 'file' table.
        String drsObjectId = BigQueryFixtures.queryForDrsId(bigQueryReader,
            snapshotModel,
            "file",
            "file_ref");

        // Step 6. Use DRS API to lookup the file by DRS ID (pulled out of the URI).
        DRSObject drsObject = dataRepoFixtures.drsGetObject(reader(), drsObjectId);
        String gsuri = TestUtils.validateDrsAccessMethods(drsObject.getAccessMethods());

        // Step 7. Try to read the file of the gs path as reader and discoverer
        String[] strings = gsuri.split("/", 4);

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
