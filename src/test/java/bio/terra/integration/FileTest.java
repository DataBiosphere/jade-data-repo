package bio.terra.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
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
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class FileTest extends UsersBase {

  private static Logger logger = LoggerFactory.getLogger(FileTest.class);

  @Autowired private AuthService authService;

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private TestConfiguration testConfiguration;

  @Rule @Autowired public TestJobWatcher testWatcher;

  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID snapshotId;
  private List<String> fileIds;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "file-acl-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    snapshotId = null;
    fileIds = new ArrayList<>();
    logger.info("created dataset " + datasetId);
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
  }

  @After
  public void tearDown() throws Exception {
    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshot(custodian(), snapshotId);
    }
    if (datasetId != null) {
      fileIds.forEach(
          f -> {
            try {
              dataRepoFixtures.deleteFile(steward(), datasetId, f);
            } catch (Exception e) {
              e.printStackTrace();
            }
          });
      dataRepoFixtures.deleteDataset(steward(), datasetId);
    }
    if (profileId != null) {
      dataRepoFixtures.deleteProfile(steward(), profileId);
    }
  }

  // The purpose of this test is to have a long-running workload that completes successfully
  // while we delete pods and have them recover.
  // Marked ignore for normal testing.
  @Ignore
  @Test
  public void longFileLoadTest() throws Exception {
    // TODO: want this to run about 5 minutes on 2 DRmanager instances. The speed of loads is when
    // they are
    //  not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do 2.5
    // files per minute,
    //  so two instances should do 5 files per minute. To run 5 minutes we should run 25 files.
    //  (There are 25 files in the directory, so if we need more we should do a reuse scheme like
    // the fileLoadTest)
    final int filesToLoad = 25;

    String loadTag = Names.randomizeName("longtest");

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(profileId)
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    logger.info("longFileLoadTest loading " + filesToLoad + " files into dataset id " + datasetId);

    for (int i = 0; i < filesToLoad; i++) {
      String tailPath = String.format("/fileloadscaletest/file1GB-%02d.txt", i);
      String sourcePath = "gs://jade-testdata-uswestregion" + tailPath;
      String targetPath = "/" + loadTag + tailPath;

      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    BulkLoadArrayResultModel result =
        dataRepoFixtures.bulkLoadArray(steward(), datasetId, arrayLoad);
    BulkLoadResultModel loadSummary = result.getLoadSummary();
    logger.info("Total files    : " + loadSummary.getTotalFiles());
    logger.info("Succeeded files: " + loadSummary.getSucceededFiles());
    logger.info("Failed files   : " + loadSummary.getFailedFiles());
    logger.info("Not Tried files: " + loadSummary.getNotTriedFiles());
  }

  // DR-612 filesystem corruption test; use a non-existent file to make sure everything errors
  // Do file ingests in parallel using a filename that will cause failure
  @Test
  public void fileParallelFailedLoadTest() throws Exception {
    List<DataRepoResponse<JobModel>> responseList = new ArrayList<>();
    String gsPath = "gs://" + testConfiguration.getIngestbucket() + "/nonexistentfile";
    String filePath = "/foo" + UUID.randomUUID() + "/bar";

    for (int i = 0; i < 20; i++) {
      DataRepoResponse<JobModel> launchResp =
          dataRepoFixtures.ingestFileLaunch(steward(), datasetId, profileId, gsPath, filePath + i);
      responseList.add(launchResp);
    }

    int failureCount = 0;
    for (DataRepoResponse<JobModel> resp : responseList) {
      DataRepoResponse<FileModel> response =
          dataRepoClient.waitForResponse(steward(), resp, new TypeReference<>() {});
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
  @SuppressFBWarnings(
      value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
      justification = "Spurious RCN check; related to Java 11")
  public void fileUnauthorizedPermissionsTest() throws Exception {

    String gsPath = "gs://" + testConfiguration.getIngestbucket();
    String filePath = "/foo/bar";

    FileModel fileModel =
        dataRepoFixtures.ingestFile(
            steward(), datasetId, profileId, gsPath + "/files/File Design Notes.pdf", filePath);
    String fileId = fileModel.getFileId();

    String json = String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileId);

    String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
    BlobInfo targetBlobInfo =
        BlobInfo.newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath)).build();

    Storage storage = StorageOptions.getDefaultInstance().getService();
    try (WriteChannel writer = storage.writer(targetBlobInfo)) {
      writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
    }

    IngestRequestModel request = dataRepoFixtures.buildSimpleIngest("file", targetPath);
    IngestResponseModel ingestResponseModel =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    assertThat("1 Row was ingested", ingestResponseModel.getRowCount(), equalTo(1L));

    // validates success
    dataRepoFixtures.getFileById(steward(), datasetId, fileId);
    dataRepoFixtures.getFileById(custodian(), datasetId, fileId);

    DataRepoResponse<FileModel> readerResp =
        dataRepoFixtures.getFileByIdRaw(reader(), datasetId, fileId);
    assertThat(
        "Reader is not authorized to get a file from a dataset",
        readerResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // get file by id
    DataRepoResponse<FileModel> discovererResp =
        dataRepoFixtures.getFileByIdRaw(discoverer(), datasetId, fileId);
    assertThat(
        "Discoverer is not authorized to get a file from a dataset",
        discovererResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // get file by name validates success
    dataRepoFixtures.getFileByName(steward(), datasetId, filePath);
    dataRepoFixtures.getFileByName(custodian(), datasetId, filePath);

    readerResp = dataRepoFixtures.getFileByNameRaw(reader(), datasetId, filePath);
    assertThat(
        "Reader is not authorized to get a file from a dataset",
        readerResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    discovererResp = dataRepoFixtures.getFileByNameRaw(discoverer(), datasetId, filePath);
    assertThat(
        "Discoverer is not authorized to get file",
        discovererResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // delete
    DataRepoResponse<JobModel> job = dataRepoFixtures.deleteFileLaunch(reader(), datasetId, fileId);
    assertThat(
        "Reader is not authorized to delete file",
        job.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // validates success
    dataRepoFixtures.deleteFile(custodian(), datasetId, fileId);
  }

  @Test
  public void fileUncommonNameTest() throws Exception {
    String gsPath = "gs://" + testConfiguration.getIngestbucket();
    String filePath = "/foo/bar";

    FileModel fileModel =
        dataRepoFixtures.ingestFile(
            steward(),
            datasetId,
            profileId,
            gsPath + "/files/file with space and #hash%percent+plus.txt",
            filePath);
    String fileId = fileModel.getFileId();

    int numRows = 1000;
    String json =
        IntStream.range(0, numRows)
            .mapToObj(i -> String.format("{\"file_id\":\"foo\",\"file_ref\":\"%s\"}", fileId))
            .collect(Collectors.joining("\n"));

    String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";
    BlobInfo targetBlobInfo =
        BlobInfo.newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath)).build();

    Storage storage = StorageOptions.getDefaultInstance().getService();
    try (WriteChannel writer = storage.writer(targetBlobInfo)) {
      writer.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
    }

    IngestRequestModel request = dataRepoFixtures.buildSimpleIngest("file", targetPath);
    IngestResponseModel ingestResponseModel =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    assertThat(
        "right number of rows were  ingested",
        ingestResponseModel.getRowCount(),
        equalTo((long) numRows));

    // Create a snapshot exposing the one row and grant read access to our reader.
    SnapshotSummaryModel snapshotSummaryModel =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "file-acl-test-snapshot.json");
    snapshotId = snapshotSummaryModel.getId();

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    // Use DRS API to lookup the file by DRS ID
    String drsObjectId = String.format("v1_%s_%s", snapshotId, fileId);
    // Should fail due to insufficient permissions
    assertThatThrownBy(() -> dataRepoFixtures.drsGetObject(steward(), drsObjectId));
    DRSObject drsObject = dataRepoFixtures.drsGetObject(custodian(), drsObjectId);

    logger.info("Drs Object: {}", drsObject);

    TestUtils.validateDrsAccessMethods(
        drsObject.getAccessMethods(), authService.getDirectAccessAuthToken(custodian().getEmail()));
  }

  @Test
  public void fileIngestAccessTest() throws Exception {
    String gsPath = "gs://" + testConfiguration.getIngestbucket();
    String filePath = "/foo/bar";
    String gsFilePath = gsPath + "/files/file with space and #hash%percent+plus.txt";
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, reader().getEmail());
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, reader().getEmail(), IamResourceType.SPEND_PROFILE);
    DataRepoResponse<JobModel> ingestJob =
        dataRepoFixtures.ingestFileLaunch(
            // note: reader's proxy group should not have access to the source bucket
            reader(), datasetId, profileId, gsFilePath, filePath);
    DataRepoResponse<FileModel> error =
        dataRepoClient.waitForResponse(steward(), ingestJob, new TypeReference<>() {});

    assertThat(error.getErrorObject().isPresent(), equalTo(true));
    assertThat(
        error.getErrorObject().get().getMessage(),
        containsString(
            "Accessing bucket " + testConfiguration.getIngestbucket() + " is not authorized"));

    // To be safe, make sure that ingest works for a steward
    dataRepoFixtures.ingestFile(
        // note: steward's proxy group should have access to the source bucket
        steward(), datasetId, profileId, gsFilePath, filePath);
  }

  @Test
  public void fileIngestBadTargetPathTest() throws Exception {
    String gsPath = "gs://" + testConfiguration.getIngestbucket();
    String filePath = "foo/bar";

    DataRepoResponse<JobModel> job =
        dataRepoFixtures.ingestFileLaunch(
            steward(), datasetId, profileId, gsPath + "/files/File Design Notes.pdf", filePath);

    DataRepoResponse<FileModel> result =
        dataRepoClient.waitForResponse(steward(), job, new TypeReference<>() {});

    // The weird URL encoding of errors means we can't match on the '/'.
    assertThat(
        "an target path without leading '/' fails to load",
        result.getErrorObject().orElseThrow().getMessage(),
        containsString("A target path must start with"));
  }
}
