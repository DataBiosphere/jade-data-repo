package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.OnDemand;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.GcsFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.configuration.ConfigEnum;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(OnDemand.class)
public class AccessTest extends UsersBase {
  private static final Logger logger = LoggerFactory.getLogger(AccessTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private IamProviderInterface iamService;
  @Autowired private TestConfiguration testConfiguration;

  private String discovererToken;
  private String readerToken;
  private String custodianToken;
  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID profileId;
  private List<UUID> snapshotIds;

  @Before
  public void setup() throws Exception {
    super.setup();
    discovererToken = authService.getDirectAccessAuthToken(discoverer().getEmail());
    readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
    custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    datasetId = null;
    snapshotIds = new ArrayList<>();
  }

  @After
  public void teardown() throws Exception {
    for (UUID snapshotId : snapshotIds) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }
    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }
  }

  private void makeIngestTestDataset() throws Exception {
    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
  }

  private void makeAclTestDataset() throws Exception {
    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "file-acl-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
  }

  private Storage getStorage(String token) {
    GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
    return GcsFixtures.getStorage(googleCredentials);
  }

  @Test
  public void checkShared() throws Exception {
    makeIngestTestDataset();
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

    String datasetBqSnapshotName = "datarepo_" + dataset.getName();

    BigQuery custodianBigQuery =
        BigQueryFixtures.getBigQuery(dataset.getDataProject(), custodianToken);
    try {
      BigQueryFixtures.datasetExists(
          custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);
      fail("custodian shouldn't be able to access bq dataset before it is shared with them");
    } catch (IllegalStateException e) {
      assertThat(
          "checking message for pdao exception error",
          e.getMessage(),
          equalTo("existence check failed for " + datasetBqSnapshotName));
    }

    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
    DataRepoResponse<EnumerateDatasetModel> enumDatasets =
        dataRepoFixtures.enumerateDatasetsRaw(custodian());
    assertThat(
        "Custodian is authorized to enumerate datasets",
        enumDatasets.getStatusCode(),
        equalTo(HttpStatus.OK));

    BigQueryFixtures.assertBqDatasetAccessible(
        custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);

    SnapshotSummaryModel snapshotSummaryModel =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");

    SnapshotModel snapshotModel =
        dataRepoFixtures.getSnapshot(
            custodian(),
            snapshotSummaryModel.getId(),
            List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION));
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), readerToken);
    try {
      BigQueryFixtures.datasetExists(
          bigQuery,
          snapshotModel.getAccessInformation().getBigQuery().getProjectId(),
          snapshotModel.getAccessInformation().getBigQuery().getDatasetName());
      fail("reader shouldn't be able to access bq dataset before it is shared with them");
    } catch (IllegalStateException e) {
      assertThat(
          "checking message for exception error",
          e.getMessage(),
          equalTo("existence check failed for ".concat(snapshotSummaryModel.getName())));
    }

    dataRepoFixtures.addSnapshotPolicyMember(
        custodian(), snapshotSummaryModel.getId(), IamRole.READER, reader().getEmail());

    AuthenticatedUserRequest authenticatedReaderRequest =
        AuthenticatedUserRequest.builder()
            .setEmail(reader().getEmail())
            .setToken(readerToken)
            .build();
    assertThat(
        "correctly added reader",
        iamService.isAuthorized(
            authenticatedReaderRequest,
            IamResourceType.DATASNAPSHOT,
            snapshotSummaryModel.getId().toString(),
            IamAction.READ_DATA),
        equalTo(true));

    BigQueryFixtures.assertBqDatasetAccessible(
        bigQuery, snapshotModel.getDataProject(), snapshotModel.getName());
  }

  @Test
  public void fileAclTest() throws Exception {
    makeAclTestDataset();

    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetSummaryModel.getId(), IamRole.CUSTODIAN, custodian().getEmail());

    // Ingest a file into the dataset
    String gsPath = "gs://" + testConfiguration.getIngestbucket();
    FileModel fileModel =
        dataRepoFixtures.ingestFile(
            steward(),
            datasetSummaryModel.getId(),
            profileId,
            gsPath + "/files/File Design Notes.pdf",
            "/foo/bar");

    // Ingest one row into the study 'file' table with a reference to that ingested file
    String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileModel.getFileId());
    String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
    BlobInfo targetBlobInfo =
        BlobInfo.newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath)).build();

    Storage storage = StorageOptions.getDefaultInstance().getService();
    try (WriteChannel writer = storage.writer(targetBlobInfo)) {
      writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
    }

    IngestRequestModel request = dataRepoFixtures.buildSimpleIngest("file", targetPath);
    IngestResponseModel ingestResponseModel =
        dataRepoFixtures.ingestJsonData(steward(), datasetSummaryModel.getId(), request);

    assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

    // Create a snapshot exposing the one row and grant read access to our reader.
    SnapshotSummaryModel snapshotSummaryModel =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "file-acl-test-snapshot.json");
    snapshotIds.add(snapshotSummaryModel.getId());
    SnapshotModel snapshotModel =
        dataRepoFixtures.getSnapshot(custodian(), snapshotSummaryModel.getId(), null);

    dataRepoFixtures.addSnapshotPolicyMember(
        custodian(), snapshotModel.getId(), IamRole.READER, reader().getEmail());

    AuthenticatedUserRequest authenticatedReaderRequest =
        AuthenticatedUserRequest.builder()
            .setEmail(reader().getEmail())
            .setToken(readerToken)
            .build();
    boolean authorized =
        iamService.isAuthorized(
            authenticatedReaderRequest,
            IamResourceType.DATASNAPSHOT,
            snapshotModel.getId().toString(),
            IamAction.READ_DATA);
    assertTrue("correctly added reader", authorized);

    // The reader does not have permission to make queries in any project,
    // so we have to use the custodian to look up the DRS id.
    BigQuery bigQueryCustodian =
        BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), custodianToken);
    BigQueryFixtures.assertBqDatasetAccessible(
        bigQueryCustodian, snapshotModel.getDataProject(), snapshotModel.getName());

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    // Read and validate the DRS URI from the file ref column in the 'file' table.
    String drsObjectId =
        dataRepoFixtures.retrieveDrsIdFromSnapshotPreview(
            reader(), snapshotModel.getId(), "file", "file_ref");

    // Use DRS API to lookup the file by DRS ID (pulled out of the URI).
    DRSObject drsObject = dataRepoFixtures.drsGetObject(reader(), drsObjectId);
    String gsuri =
        TestUtils.validateDrsAccessMethods(drsObject.getAccessMethods(), custodianToken, false);

    // Try to read the file of the gs path as reader and discoverer
    String[] strings = gsuri.split("/", 4);

    String bucketName = strings[2];
    String blobName = strings[3];
    BlobId blobId = BlobId.of(bucketName, blobName);

    Storage readerStorage = getStorage(readerToken);
    assertTrue("Reader can read some bytes of the file", canReadBlobRetry(readerStorage, blobId));

    Storage discovererStorage = getStorage(discovererToken);
    assertFalse("Discoverer can not read the file", canReadBlob(discovererStorage, blobId));
  }

  @Test
  public void fileAclFaultTest() throws Exception {
    try {
      // Run the fileAclTest with the SNAPSHOT_GRANT_FILE_ACCESS_FAULT on
      dataRepoFixtures.setFault(
          steward(), ConfigEnum.SNAPSHOT_GRANT_FILE_ACCESS_FAULT.name(), true);
      fileAclTest();
    } finally {
      dataRepoFixtures.resetConfig(steward());
    }
  }

  @Test
  public void checkCustodianPermissions() throws Exception {
    makeIngestTestDataset();
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

    String datasetBqSnapshotName = PdaoConstant.PDAO_PREFIX + dataset.getName();

    BigQuery custodianBigQuery =
        BigQueryFixtures.getBigQuery(dataset.getDataProject(), custodianToken);
    try {
      BigQueryFixtures.datasetExists(
          custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);
      fail("custodian shouldn't be able to access bq dataset before it is shared with them");
    } catch (IllegalStateException e) {
      assertThat(
          "checking message for pdao exception error",
          e.getMessage(),
          equalTo("existence check failed for " + datasetBqSnapshotName));
    }

    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
    DataRepoResponse<EnumerateDatasetModel> enumDatasets =
        dataRepoFixtures.enumerateDatasetsRaw(custodian());
    assertThat(
        "Custodian is authorized to enumerate datasets",
        enumDatasets.getStatusCode(),
        equalTo(HttpStatus.OK));

    BigQueryFixtures.assertBqDatasetAccessible(
        custodianBigQuery, dataset.getDataProject(), datasetBqSnapshotName);

    // gets the "sample" table and makes a table ref to use in the query
    dataRepoFixtures.assertDatasetTableCount(
        custodian(), dataset, dataset.getSchema().getTables().get(1).getName(), 7);
  }

  private boolean canReadBlob(Storage storage, BlobId blobId) throws Exception {
    try (ReadChannel reader = storage.reader(blobId)) {
      ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
      int bytesRead = reader.read(bytes);
      return (bytesRead > 0);
    } catch (Exception ex) {
      logger.info("Caught exception", ex);
    }
    return false;
  }

  private static final int RETRY_INITIAL_INTERVAL_SECONDS = 2;
  private static final int RETRY_MAX_INTERVAL_SECONDS = 30;
  private static final int RETRY_MAX_SLEEP_SECONDS = 420;

  private boolean canReadBlobRetry(Storage storage, BlobId blobId) throws Exception {
    int sleptSeconds = 0;
    int sleepSeconds = RETRY_INITIAL_INTERVAL_SECONDS;
    while (true) {
      try (ReadChannel reader = storage.reader(blobId)) {
        ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
        int bytesRead = reader.read(bytes);
        return (bytesRead > 0);
      } catch (Exception ex) {
        logger.info("Caught IO exception: " + ex.getMessage());
        if ((sleptSeconds < RETRY_MAX_SLEEP_SECONDS)
            && StringUtils.contains(ex.getMessage(), "Forbidden")) {

          TimeUnit.SECONDS.sleep(sleepSeconds);
          sleptSeconds += sleepSeconds;
          logger.info("Slept " + sleepSeconds + " total slept " + sleptSeconds);
          sleepSeconds = Math.min(2 * sleepSeconds, RETRY_MAX_INTERVAL_SECONDS);
        } else {
          throw ex;
        }
      }
    }
  }
}
