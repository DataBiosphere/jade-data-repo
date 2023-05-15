package bio.terra.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class FileTest extends UsersBase {

  private static Logger logger = LoggerFactory.getLogger(FileTest.class);

  private static final int NUM_FILES = 100;
  private static final int NUM_FAILED_FILES = 5;

  @Autowired private AuthService authService;

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private TestConfiguration testConfiguration;

  @MockBean private JobService jobService;

  @Rule @Autowired public TestJobWatcher testWatcher;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  private ObjectMapper objectMapper;
  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID snapshotId;
  private List<String> fileIds;
  private UUID profileId;
  private BlobId controlFileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    controlFileId = null;

    // Make sure we can write flat json objects
    DefaultPrettyPrinter p = new DefaultPrettyPrinter();
    DefaultPrettyPrinter.Indenter i = new DefaultIndenter("", "");
    p.indentArraysWith(i);
    p.indentObjectsWith(i);
    objectMapper = new ObjectMapper().setDefaultPrettyPrinter(p);
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
    if (controlFileId != null) {
      storage.delete(controlFileId);
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

  // The purpose of these tests is to ingest files using the bulk mode in various permutations
  @Test
  public void bulkFileLoadTestTdrHostedRandomIdFile() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, false);
  }

  @Test
  public void bulkFileLoadTestTdrHostedRandomIdFileHandlesMaxFailedFiles() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, false, NUM_FAILED_FILES, NUM_FAILED_FILES);
  }

  @Test
  public void bulkFileLoadTestTdrHostedRandomIdFileWithZeroMaxFailedFiles() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, false, NUM_FAILED_FILES, 0);
  }

  @Test
  public void bulkFileLoadTestTdrHostedRandomIdArray() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, true);
  }

  @Test
  public void bulkFileLoadTestTdrHostedRandomIdArrayHandlesMaxFailedFiles() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, true, NUM_FAILED_FILES, NUM_FAILED_FILES);
  }

  @Test
  public void bulkFileLoadTestTdrHostedRandomIdArrayZeroMaxFailedFiles() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, false, true, 1, 0);
  }

  @Test
  public void bulkFileLoadTestTdrHostedPredictableIdFile() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, true, false);
  }

  @Test
  public void bulkFileLoadTestTdrHostedPredictableIdArray() throws Exception {
    bulkFileLoadTest(NUM_FILES, false, true, true);
  }

  @Test
  public void bulkFileLoadTestSelfHostedRandomIdFile() throws Exception {
    bulkFileLoadTest(NUM_FILES, true, false, false);
  }

  @Test
  public void bulkFileLoadTestSelfHostedRandomIdArray() throws Exception {
    bulkFileLoadTest(NUM_FILES, true, false, true);
  }

  @Test
  public void bulkFileLoadTestSelfHostedPredictableIdFile() throws Exception {
    bulkFileLoadTest(NUM_FILES, true, true, false);
  }

  @Test
  public void bulkFileLoadTestSelfHostedPredictableIdArray() throws Exception {
    bulkFileLoadTest(NUM_FILES, true, true, true);
  }

  @Test
  public void bulkFileLoadTestSelfHostedPredictableIdMoveSourceFiles() throws Exception {
    // Run through basic ingest
    String loadTag = bulkFileLoadTest(NUM_FILES, true, true, true);
    String originalSourcePath = "gs://jade-testdata-uswestregion/fileloadprofiletest/1KBfile.txt";
    String newSourcePath = "gs://jade-testdata-uswestregion/fileloadprofiletest/1KBfile.moved.txt";

    // Get the list of ingested files
    BulkLoadHistoryModelList loadHistory =
        dataRepoFixtures.getLoadHistory(steward(), datasetId, loadTag, 0, NUM_FILES * 2);

    assertThat(
        "the right amount of files are in the load history table",
        loadHistory.getItems(),
        hasSize(NUM_FILES));
    // Assert that they all point to the same (1KB) source file
    List<String> sourceFiles =
        loadHistory.getItems().stream()
            .map(BulkLoadHistoryModel::getSourcePath)
            .distinct()
            .toList();
    assertThat("a single source file was ingested", sourceFiles, hasSize(1));
    assertThat(
        "the file path points to the expected path",
        sourceFiles.get(0),
        equalTo(originalSourcePath));

    // Now re-ingest 2 files in a new location (e.g. re-point them)
    List<BulkLoadFileModel> reloadArray =
        loadHistory.getItems().subList(0, 2).stream()
            .map(
                f ->
                    new BulkLoadFileModel()
                        .sourcePath(newSourcePath)
                        .targetPath(f.getTargetPath())
                        .mimeType("application/binary"))
            .toList();

    BulkLoadHistoryModel notReIngestedFile = loadHistory.getItems().get(2);

    BulkLoadArrayRequestModel bulkLoad =
        new BulkLoadArrayRequestModel()
            .profileId(profileId)
            .loadTag(loadTag)
            .bulkMode(true)
            .loadArray(reloadArray)
            .maxFailedFileLoads(reloadArray.size());

    BulkLoadArrayResultModel bulkLoadArrayResultModel =
        dataRepoFixtures.bulkLoadArray(steward(), datasetId, bulkLoad);
    assertThat(
        "the right number of files were loaded",
        bulkLoadArrayResultModel.getLoadSummary().getSucceededFiles(),
        equalTo(2));

    // iterate over the moved files and ensure that their source is the new path
    for (var file : reloadArray) {
      FileModel dsFile = dataRepoFixtures.getFileByName(steward(), datasetId, file.getTargetPath());
      assertThat(
          "source file was moved", dsFile.getFileDetail().getAccessUrl(), equalTo(newSourcePath));
    }
    // verify that one of the files that WASN'T re-ingest still points to the original source file
    FileModel dsFile =
        dataRepoFixtures.getFileByName(steward(), datasetId, notReIngestedFile.getTargetPath());
    assertThat(
        "source file was not moved",
        dsFile.getFileDetail().getAccessUrl(),
        equalTo(originalSourcePath));
  }

  private String bulkFileLoadTest(
      int filesToLoad, boolean selfHosted, boolean predictableFileIds, boolean arrayIngestMode)
      throws Exception {
    return bulkFileLoadTest(
        filesToLoad, selfHosted, predictableFileIds, arrayIngestMode, 0, filesToLoad);
  }

  // Return the load tag used to ingest
  private String bulkFileLoadTest(
      int filesToLoad,
      boolean selfHosted,
      boolean predictableFileIds,
      boolean arrayIngestMode,
      int filesToFail,
      int maxFailedFileLoads)
      throws Exception {
    initialize(selfHosted, predictableFileIds);

    String loadTag = Names.randomizeName("longtest");
    BulkLoadResultModel loadSummary = null;
    long start;
    if (arrayIngestMode) {
      BulkLoadArrayRequestModel arrayLoad =
          new BulkLoadArrayRequestModel()
              .profileId(profileId)
              .loadTag(loadTag)
              .bulkMode(true)
              .maxFailedFileLoads(maxFailedFileLoads);

      logger.info(
          "bulkFileLoadTest loading " + filesToLoad + " files into dataset id " + datasetId);

      int failedFileLoadModels = 0;
      for (int i = 0; i < filesToLoad; i++) {
        String tailPath = "/fileloadprofiletest/1KBfile.txt";
        if (failedFileLoadModels < filesToFail) {
          tailPath = "/foo/foo.txt";
          failedFileLoadModels += 1;
        }
        String sourcePath = "gs://jade-testdata-uswestregion" + tailPath;
        String targetPath = "/" + loadTag + "/" + i + tailPath;

        BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
        model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
        arrayLoad.addLoadArrayItem(model);
      }
      start = Instant.now().toEpochMilli();

      if (maxFailedFileLoads < filesToFail) {
        ErrorModel errorModel =
            dataRepoFixtures.bulkLoadArrayFailure(steward(), datasetId, arrayLoad);
        assertThat(
            errorModel.getMessage(),
            containsString("More than " + maxFailedFileLoads + " file(s) failed to ingest"));
      } else {
        BulkLoadArrayResultModel result =
            dataRepoFixtures.bulkLoadArray(steward(), datasetId, arrayLoad);
        loadSummary = result.getLoadSummary();
      }
    } else {
      String controlFilePath =
          "gs://jade-testdata-uswestregion/scratch/file-test-control.%s.json"
              .formatted(Instant.now().toEpochMilli());
      BulkLoadRequestModel bulkLoad =
          new BulkLoadRequestModel()
              .profileId(profileId)
              .loadTag(loadTag)
              .bulkMode(true)
              .maxFailedFileLoads(maxFailedFileLoads)
              .loadControlFile(controlFilePath);

      // Write ingest control file
      controlFileId = BlobId.fromGsUtilUri(controlFilePath);

      StringBuilder sb = new StringBuilder();
      int failedFileLoadModels = 0;
      for (int i = 0; i < filesToLoad; i++) {
        String tailPath = "/fileloadprofiletest/1KBfile.txt";
        if (failedFileLoadModels < filesToFail) {
          tailPath = "/foo/foo.txt";
          failedFileLoadModels += 1;
        }
        String sourcePath = "gs://jade-testdata-uswestregion" + tailPath;
        String targetPath = "/" + loadTag + "/" + i + tailPath;

        BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
        model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
        sb.append(objectMapper.writeValueAsString(model)).append("\n");
      }

      BlobInfo controlFile =
          BlobInfo.newBuilder(controlFileId)
              .setContentType(MediaType.APPLICATION_JSON_VALUE)
              .build();
      storage.create(controlFile, sb.toString().getBytes(StandardCharsets.UTF_8));

      start = Instant.now().toEpochMilli();
      if (maxFailedFileLoads < filesToFail) {
        ErrorModel errorModel = dataRepoFixtures.bulkLoadFailure(steward(), datasetId, bulkLoad);
        assertThat(
            errorModel.getMessage(),
            containsString("More than " + maxFailedFileLoads + " file(s) failed to ingest"));
      } else {
        loadSummary = dataRepoFixtures.bulkLoad(steward(), datasetId, bulkLoad);
      }
    }
    logger.info("Ingest took %s milliseconds".formatted(Instant.now().toEpochMilli() - start));
    if (loadSummary != null) {
      logger.info("Total files    : " + loadSummary.getTotalFiles());
      logger.info("Succeeded files: " + loadSummary.getSucceededFiles());
      logger.info("Failed files   : " + loadSummary.getFailedFiles());
      logger.info("Not Tried files: " + loadSummary.getNotTriedFiles());

      assertThat(
          "all files should succeed",
          loadSummary.getSucceededFiles(),
          equalTo(filesToLoad - filesToFail));
      assertThat("all files should succeed", loadSummary.getFailedFiles(), equalTo(filesToFail));
    }
    return loadTag;
  }

  // DR-612 filesystem corruption test; use a non-existent file to make sure everything errors
  // Do file ingests in parallel using a filename that will cause failure
  @Test
  public void fileParallelFailedLoadTest() throws Exception {
    initialize(false, false);
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
    initialize(false, false);
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
        equalTo(HttpStatus.FORBIDDEN));

    // get file by id
    DataRepoResponse<FileModel> discovererResp =
        dataRepoFixtures.getFileByIdRaw(discoverer(), datasetId, fileId);
    assertThat(
        "Discoverer is not authorized to get a file from a dataset",
        discovererResp.getStatusCode(),
        equalTo(HttpStatus.FORBIDDEN));

    // get file by name validates success
    dataRepoFixtures.getFileByName(steward(), datasetId, filePath);
    dataRepoFixtures.getFileByName(custodian(), datasetId, filePath);

    readerResp = dataRepoFixtures.getFileByNameRaw(reader(), datasetId, filePath);
    assertThat(
        "Reader is not authorized to get a file from a dataset",
        readerResp.getStatusCode(),
        equalTo(HttpStatus.FORBIDDEN));

    discovererResp = dataRepoFixtures.getFileByNameRaw(discoverer(), datasetId, filePath);
    assertThat(
        "Discoverer is not authorized to get file",
        discovererResp.getStatusCode(),
        equalTo(HttpStatus.FORBIDDEN));

    // delete
    DataRepoResponse<JobModel> job = dataRepoFixtures.deleteFileLaunch(reader(), datasetId, fileId);
    assertThat(
        "Reader is not authorized to delete file",
        job.getStatusCode(),
        equalTo(HttpStatus.FORBIDDEN));

    // validates success
    dataRepoFixtures.deleteFile(custodian(), datasetId, fileId);
  }

  @Test
  public void fileUncommonNameTest() throws Exception {
    initialize(false, false);
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
        drsObject.getAccessMethods(),
        authService.getDirectAccessAuthToken(custodian().getEmail()),
        false);
  }

  @Test
  public void fileIngestAccessTest() throws Exception {
    initialize(false, false);
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
    initialize(false, false);
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

  private void initialize(boolean selfHosted, boolean predictableFileIds) throws Exception {
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    DataRepoResponse<JobModel> datasetCreateJob =
        dataRepoFixtures.createDatasetRaw(
            steward(),
            profileId,
            "file-acl-test-dataset.json",
            CloudPlatform.GCP,
            false,
            selfHosted,
            false,
            predictableFileIds,
            new DatasetRequestModelPolicies());

    datasetSummaryModel = dataRepoFixtures.waitForDatasetCreate(steward(), datasetCreateJob);
    datasetId = datasetSummaryModel.getId();
    snapshotId = null;
    fileIds = new ArrayList<>();
    logger.info("created dataset " + datasetId);
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
  }
}
