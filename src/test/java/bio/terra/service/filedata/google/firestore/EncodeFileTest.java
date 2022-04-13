package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.StringListCompare;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.FileModelType;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.BufferedReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class EncodeFileTest {
  private static final Logger logger = LoggerFactory.getLogger(EncodeFileTest.class);

  @Autowired private MockMvc mvc;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private GoogleResourceManagerService resourceManagerService;
  @Autowired private DrsIdService drsIdService;
  @Autowired private BufferService bufferService;

  private static final String ID_GARBAGE = "GARBAGE";

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profileModel;
  private Storage storage;
  private String targetProjectId;
  private String loadTag;
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);
    String coreBillingAccountId = testConfig.getGoogleBillingAccountId();
    profileModel = connectedOperations.createProfileForAccount(coreBillingAccountId);
    loadTag = "encodeLoadTag" + UUID.randomUUID();
    datasetSummary = connectedOperations.createDataset(profileModel, "encodefiletest-dataset.json");
    ResourceInfo resourceInfo =
        bufferService.handoutResource(datasetSummary.isSecureMonitoringEnabled());
    targetProjectId = resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    resourceManagerService.addLabelsToProject(
        targetProjectId, Map.of("test-name", "encode-file-test"));
    // Build a storage object for the data project of the dataset.
    StorageOptions storageOptions =
        StorageOptions.newBuilder().setProjectId(targetProjectId).build();
    storage = storageOptions.getService();
    logger.info("--------begin test---------");
  }

  @After
  public void teardown() throws Exception {
    logger.info("--------start of tear down---------");
    connectedOperations.teardown();
  }

  // NOTES ABOUT THESE TESTS: these tests require create access to the jade-testdata bucket in order
  // to
  // re-write the json source data replacing the gs paths with the Jade object id.
  @Test
  public void encodeFileTest() throws Exception {
    encodeTest(testConfig.getIngestbucket());
  }

  @Test
  public void encodeFileRequesterPaysTest() throws Exception {
    encodeTest(testConfig.getIngestRequesterPaysBucket());
  }

  private void encodeTest(String bucketName) throws Exception {
    // Load all of the files into the dataset
    String targetPath = loadFiles(datasetSummary.getId(), false, false, bucketName);

    String gsPath = "gs://" + bucketName + "/" + targetPath;
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("file")
            .path(gsPath);

    connectedOperations.ingestTableSuccess(datasetSummary.getId(), ingestRequest);

    // Delete the scratch blob
    Blob scratchBlob =
        storage.get(
            BlobId.of(bucketName, targetPath), Storage.BlobGetOption.userProject(targetProjectId));
    if (scratchBlob != null) {
      scratchBlob.delete(Blob.BlobSourceOption.userProject(targetProjectId));
    }

    // Load donor success
    ingestRequest.table("donor").path("gs://" + bucketName + "/encodetest/donor.json");

    connectedOperations.ingestTableSuccess(datasetSummary.getId(), ingestRequest);

    // At this point, we have files and tabular data. Let's make a snapshot!
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(datasetSummary, "encodefiletest-snapshot.json", "");

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    String fileUri = getFileRefIdFromSnapshot(snapshotSummary);
    DrsId drsId = drsIdService.fromUri(fileUri);

    DRSObject drsObject = connectedOperations.drsGetObjectSuccess(drsId.toDrsObjectId(), false);
    String filePath = drsObject.getAliases().get(0);

    FileModel fsObjById =
        connectedOperations.lookupSnapshotFileSuccess(
            snapshotSummary.getId(), drsId.getFsObjectId());
    FileModel fsObjByPath =
        connectedOperations.lookupSnapshotFileByPathSuccess(snapshotSummary.getId(), filePath, 0);
    assertThat("Retrieve snapshot file objects match", fsObjById, equalTo(fsObjByPath));
    assertThat("Load tag is stored", fsObjById.getFileDetail().getLoadTag(), equalTo(loadTag));

    // Build the reference directory name map
    String datasetPath = "/" + datasetSummary.getName();
    Map<String, List<String>> dirmap = makeDirectoryMap(datasetPath);

    testSnapEnum(dirmap, snapshotSummary.getId(), datasetPath, -1);
    testSnapEnum(dirmap, snapshotSummary.getId(), datasetPath, 0);
    testSnapEnum(dirmap, snapshotSummary.getId(), datasetPath, 6);
    testSnapEnum(dirmap, snapshotSummary.getId(), datasetPath, 3);

    // Try to delete a file with a dependency
    MvcResult result =
        mvc.perform(
                delete(
                    "/api/repository/v1/datasets/"
                        + datasetSummary.getId()
                        + "/files/"
                        + drsId.getFsObjectId()))
            .andReturn();
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
    assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));

    ErrorModel errorModel = connectedOperations.handleFailureCase(response);
    assertThat(
        "correct dependency error message",
        errorModel.getMessage(),
        containsString("used by at least one snapshot"));
  }

  //
  // /<datasetname>/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.bam
  //
  // /ENCFF180PCI.bam.bai
  //
  // /cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.bam
  //
  // /ENCFF912JKA.bam.bai
  //
  // /2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.bam
  //
  // /ENCFF538GKX.bam.bai
  //
  // /05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.bam
  //
  // /ENCFF823AJQ.bam.bai

  private static int MAX_DIRECTORY_DEPTH = 6;

  private Map<String, List<String>> makeDirectoryMap(String datasetPath) {
    Map<String, List<String>> dirmap = new HashMap<>();
    dirmap.put(datasetPath, Arrays.asList("encodetest"));
    dirmap.put(datasetPath + "/encodetest", Arrays.asList("files"));
    dirmap.put(datasetPath + "/encodetest/files", Arrays.asList("2017", "2018"));
    dirmap.put(datasetPath + "/encodetest/files/2017", Arrays.asList("08"));
    dirmap.put(datasetPath + "/encodetest/files/2017/08", Arrays.asList("24"));
    dirmap.put(
        datasetPath + "/encodetest/files/2017/08/24",
        Arrays.asList(
            "80317b07-7e78-4223-a3a2-84991c3104be", "cd3df621-4696-4fae-a2fc-2c666cafa5e2"));
    dirmap.put(
        datasetPath + "/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be",
        Arrays.asList("ENCFF180PCI.bam", "ENCFF180PCI.bam.bai"));
    dirmap.put(
        datasetPath + "/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2",
        Arrays.asList("ENCFF912JKA.bam", "ENCFF912JKA.bam.bai"));
    dirmap.put(datasetPath + "/encodetest/files/2018", Arrays.asList("01", "05"));
    dirmap.put(datasetPath + "/encodetest/files/2018/01", Arrays.asList("18"));
    dirmap.put(
        datasetPath + "/encodetest/files/2018/01/18",
        Arrays.asList("82aab61a-1e9b-43d3-8836-d9c54cf37dd6"));
    dirmap.put(
        datasetPath + "/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6",
        Arrays.asList("ENCFF538GKX.bam", "ENCFF538GKX.bam.bai"));
    dirmap.put(datasetPath + "/encodetest/files/2018/05", Arrays.asList("04"));
    dirmap.put(
        datasetPath + "/encodetest/files/2018/05/04",
        Arrays.asList("289b5fd2-ea5e-4275-a56d-2185738737e0"));
    dirmap.put(
        datasetPath + "/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0",
        Arrays.asList("ENCFF823AJQ.bam", "ENCFF823AJQ.bam.bai"));
    return dirmap;
  }

  private void testSnapEnum(
      Map<String, List<String>> dirmap, UUID snapshotId, String datasetPath, int inDepth)
      throws Exception {
    FileModel fsObj =
        connectedOperations.lookupSnapshotFileByPathSuccess(snapshotId, datasetPath, inDepth);
    int maxDepth = checkSnapEnum(dirmap, 0, fsObj);
    int depth = (inDepth == -1) ? MAX_DIRECTORY_DEPTH : inDepth;
    assertThat("Depth is correct", maxDepth, equalTo(depth));
  }

  // return is the max depth we have seen; level is the input depth so far
  private int checkSnapEnum(Map<String, List<String>> dirmap, int level, FileModel fsObj) {
    // If there are not contents, then we are at the deepest level
    List<FileModel> contentsList = fsObj.getDirectoryDetail().getContents();
    if (contentsList == null || contentsList.size() == 0) {
      return level;
    }

    // build string list from the contents objects
    List<String> contentsNames =
        contentsList.stream()
            .map(fs -> FileMetadataUtils.getName(fs.getPath()))
            .collect(Collectors.toList());

    // lookup the dirmap list by path of the fsObj
    List<String> mapList = dirmap.get(fsObj.getPath());

    // compare the lists
    StringListCompare slc = new StringListCompare(contentsNames, mapList);
    assertTrue("Directory contents match", slc.compare());

    // loop through contents; if dir, recurse (level + 1)
    int maxLevel = level;
    for (FileModel fileModel : contentsList) {
      if (fileModel.getFileType() == FileModelType.DIRECTORY) {
        int aLevel = checkSnapEnum(dirmap, level + 1, fileModel);
        if (aLevel > maxLevel) {
          maxLevel = aLevel;
        }
      }
    }

    // return max level of any dirs
    return maxLevel;
  }

  @Test
  public void encodeFileBadFileId() throws Exception {
    String bucketName = testConfig.getIngestbucket();
    String targetPath = loadFiles(datasetSummary.getId(), true, false, bucketName);
    String gsPath = "gs://" + bucketName + "/" + targetPath;

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("file")
            .path(gsPath);

    String jsonRequest = TestUtils.mapToJson(ingestRequest);
    String url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/ingest";

    MvcResult result =
        mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
            .andReturn();
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    ErrorModel ingestError = connectedOperations.handleFailureCase(response);
    assertThat(
        "correctly found bad file id",
        ingestError.getMessage(),
        containsString("Invalid file ids found"));

    List<String> errorDetails = ingestError.getErrorDetail();
    assertNotNull("Error details were returned", errorDetails);
    assertThat("Bad id was returned in details", errorDetails.get(0), containsString(ID_GARBAGE));

    // Delete the scratch blob
    Blob scratchBlob = storage.get(BlobId.of(testConfig.getIngestbucket(), targetPath));
    if (scratchBlob != null) {
      scratchBlob.delete();
    }
  }

  @Test
  public void encodeFileBadRowTest() throws Exception {
    String bucketName = testConfig.getIngestbucket();
    String targetPath = loadFiles(datasetSummary.getId(), false, true, bucketName);
    String gsPath = "gs://" + bucketName + "/" + targetPath;

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("file")
            .path(gsPath);

    String jsonRequest = TestUtils.mapToJson(ingestRequest);
    String url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/ingest";

    MvcResult result =
        mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
            .andReturn();
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

    ErrorModel ingestError = connectedOperations.handleFailureCase(response);
    // NB: this used to return 2 errors. It seems like the BQ API changed recently and now returns
    // 3, except two of
    // them are the same error with one word changed.
    assertThat(
        "correctly found bad row",
        ingestError.getMessage(),
        startsWith(String.format("Ingest control file at gs://%s/scratch/", bucketName)));

    assertThat(
        "Max number of bad file lines returned (6)", ingestError.getErrorDetail(), hasSize(6));

    // entire error message should be:
    // "Unexpected character (';' (code 59)): was expecting a colon to separate field name
    //   and value at [Source: (String)\"{\"fribbitz\";\"ABCDEFG\"}\"; line: 1, column: 13]";
    String expectedError = "Unexpected character";
    assertThat(
        "Json parsing errors are present",
        ingestError.getErrorDetail().get(0),
        containsString(expectedError));

    assertThat(
        "all errors are the same plus truncate message",
        Set.copyOf(ingestError.getErrorDetail()),
        hasSize(2));

    // Delete the scratch blob
    Blob scratchBlob = storage.get(BlobId.of(bucketName, targetPath));
    if (scratchBlob != null) {
      scratchBlob.delete();
    }
  }

  private String loadFiles(
      UUID datasetId, boolean insertBadId, boolean insertBadRow, String bucketName)
      throws Exception {
    // Open the source data from the bucket
    // Open target data in bucket
    // Read one line at a time - unpack into pojo
    // Ingest the files, substituting the file ids
    // Generate JSON and write the line to scratch
    String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";

    // For a bigger test use encodetest/file.json (1000+ files)
    // For normal testing encodetest/file_small.json (10 files)
    Blob sourceBlob =
        storage.get(
            BlobId.of(bucketName, "encodetest/file_small.json"),
            Storage.BlobGetOption.userProject(targetProjectId));
    assertNotNull("source blob not null", sourceBlob);

    BlobInfo targetBlobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, targetPath)).build();

    try (WriteChannel writer =
            storage.writer(targetBlobInfo, Storage.BlobWriteOption.userProject(targetProjectId));
        BufferedReader reader =
            new BufferedReader(
                Channels.newReader(
                    sourceBlob.reader(Blob.BlobSourceOption.userProject(targetProjectId)),
                    "UTF-8"))) {

      boolean badIdInserted = false;
      boolean badRowInserted = false;
      String line = null;
      while ((line = reader.readLine()) != null) {
        EncodeFileIn encodeFileIn = TestUtils.mapFromJson(line, EncodeFileIn.class);

        String bamFileId = null;
        String bamiFileId = null;

        if (encodeFileIn.getFile_gs_path() != null) {
          FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_gs_path());
          FileModel bamFile = connectedOperations.ingestFileSuccess(datasetId, fileLoadModel);
          // Fault insertion on request: we corrupt one id if requested to do so.
          if (insertBadId && !badIdInserted) {
            bamFileId = bamFile.getFileId() + ID_GARBAGE;
            badIdInserted = true;
          } else {
            bamFileId = bamFile.getFileId();
          }
        }

        if (encodeFileIn.getFile_index_gs_path() != null) {
          FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_index_gs_path());
          FileModel bamiFile = connectedOperations.ingestFileSuccess(datasetId, fileLoadModel);
          bamiFileId = bamiFile.getFileId();
        }

        EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
        String fileLine;
        if (insertBadRow && !badRowInserted) {
          fileLine = "{\"fribbitz\";\"ABCDEFG\"}\n";
        } else {
          fileLine = TestUtils.mapToJson(encodeFileOut) + "\n";
        }
        writer.write(ByteBuffer.wrap(fileLine.getBytes("UTF-8")));
      }
    }

    return targetPath;
  }

  private String getFileRefIdFromSnapshot(SnapshotSummaryModel snapshotSummary)
      throws InterruptedException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotSummary.getName());
    String googleProjectId = snapshot.getProjectResource().getGoogleProjectId();
    BigQueryProject bigQueryProject = BigQueryProject.get(googleProjectId);

    StringBuilder builder =
        new StringBuilder()
            .append("SELECT file_ref FROM `")
            .append(googleProjectId)
            .append('.')
            .append(snapshot.getName())
            .append(".file` AS T")
            .append(" WHERE T.file_ref IS NOT NULL LIMIT 1");

    String sql = builder.toString();
    try {
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
      TableResult result = bigQueryProject.getBigQuery().query(queryConfig);
      FieldValueList row = result.iterateAll().iterator().next();
      FieldValue idValue = row.get(0);
      String drsUri = idValue.getStringValue();
      return drsUri;
    } catch (InterruptedException ie) {
      throw new PdaoException("get file ref id from snapshot unexpectedly interrupted", ie);
    }
  }

  private FileLoadModel makeFileLoadModel(String gspath) throws Exception {
    URI uri = URI.create(gspath);
    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(gspath)
            .profileId(profileModel.getId())
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(uri.getPath())
            .loadTag(loadTag);

    return fileLoadModel;
  }
}
