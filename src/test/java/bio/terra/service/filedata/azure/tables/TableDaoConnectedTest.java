package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.Names;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.SnapshotCompute;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSource;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class TableDaoConnectedTest {
  private final Logger logger = LoggerFactory.getLogger(TableDaoConnectedTest.class);
  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired AzureUtils azureUtils;
  @Autowired TableDao tableDao;
  @Autowired TableFileDao tableFileDao;
  @Autowired TableDirectoryDao tableDirectoryDao;
  private TableServiceClient tableServiceClient;
  private UUID datasetId;
  private Dataset dataset;
  private UUID snapshotId;
  private Snapshot snapshot;
  private Set<String> refIds;
  private String loadTag;
  private int numFilesToLoad;
  private String targetBasePathFormat = "/%s/%s/file-%s.json";
  private String endpoint;
  private String uniqueTestDirectory;

  @Before
  public void setUp() {
    endpoint =
        "https://"
            + connectedTestConfiguration.getSourceStorageAccountName()
            + ".table.core.windows.net";
    uniqueTestDirectory = UUID.randomUUID().toString().replace("-", "");
    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    connectedTestConfiguration.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(endpoint)
            .buildClient();
    datasetId = UUID.randomUUID();
    dataset = new Dataset().id(datasetId).name(Names.randomizeName("dataset"));
    refIds = new HashSet<>();
    snapshotId = UUID.randomUUID();
    snapshot =
        new Snapshot()
            .id(snapshotId)
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    loadTag = Names.randomizeName("loadTag");
    logger.info(
        "Test details: Dataset Name: {}, Dataset Id: {}, Snapshot Id: {}, Snapshot Table Name: {}, loadTag: {}",
        dataset.getName(),
        datasetId,
        snapshotId,
        StorageTableName.SNAPSHOT.toTableName(snapshotId),
        loadTag);

    // Add three files with the same base path
    // This will generate 7 entries in the Dataset storage table
    // These include: /, /_dr_, /_dr_/test, /_dr_/test/path
    // And one for each of the 3 files
    numFilesToLoad = 3;
    for (int i = 0; i < numFilesToLoad; i++) {
      String fileId = UUID.randomUUID().toString();
      refIds.add(fileId);
      String targetPath = String.format(targetBasePathFormat, uniqueTestDirectory, "path", i);
      createFileDirectoryEntry(fileId, targetPath);
    }

    // Add one more additional file that lives at a partially different path
    // Only 2 new entries will be generated b/c it shares part of the base path
    // New entries: /_dr_/test/diffpath and one for the 1 file
    String diffFileId = UUID.randomUUID().toString();
    refIds.add(diffFileId);
    createFileDirectoryEntry(
        diffFileId, String.format(targetBasePathFormat, uniqueTestDirectory, "diffpath", "diff"));
  }

  @After
  public void cleanup() {
    // delete entries from dataset
    refIds.forEach(
        refId -> {
          boolean success =
              tableDirectoryDao.deleteDirectoryEntry(
                  tableServiceClient,
                  datasetId,
                  StorageTableName.DATASET.toTableName(datasetId),
                  refId);
          logger.info("Delete {}: {}", refId, success);
        });

    // delete entire snapshot table
    tableDirectoryDao.deleteDirectoryEntriesFromCollection(
        tableServiceClient, StorageTableName.SNAPSHOT.toTableName(snapshotId));
  }

  @Test
  public void testAddFilesToSnapshot() {
    // First, make sure the directory entries exist in the dataset's storage table
    checkThatEntriesExist(datasetId, StorageTableName.DATASET.toTableName(datasetId), false);

    tableDao.addFilesToSnapshot(
        tableServiceClient,
        tableServiceClient,
        dataset.getId(),
        dataset.getName(),
        snapshot,
        refIds);

    // Now make sure that the same directory entries exist in the snapshot's storage table
    checkThatEntriesExist(snapshotId, StorageTableName.SNAPSHOT.toTableName(snapshotId), true);
  }

  private void createFileDirectoryEntry(String fileId, String targetPath) {
    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(fileId)
            .isFileRef(true)
            .path(FileMetadataUtils.getDirectoryPath(targetPath))
            .name(FileMetadataUtils.getName(targetPath))
            .datasetId(datasetId.toString())
            .loadTag(loadTag);
    tableDirectoryDao.createDirectoryEntry(
        tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(datasetId), newEntry);

    // test that directory entry now exists
    FireStoreDirectoryEntry de_after =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            targetPath);
    assertThat("FireStoreDirectoryEntry should now exist", de_after, equalTo(newEntry));
  }

  private void checkThatEntriesExist(UUID collectionId, String tableName, boolean isSnapshot) {
    // For Snapshot lookups, we include the dataset name in the file path
    // This is excluded for datasets
    String datasetNamePlaceholder = "";
    if (isSnapshot) {
      datasetNamePlaceholder = "/" + dataset.getName();
    }

    List<String> directories = new ArrayList<>();
    directories.add("/_dr_");
    directories.add(String.format("/_dr_%s/%s", datasetNamePlaceholder, uniqueTestDirectory));
    directories.add(String.format("/_dr_%s/%s/path", datasetNamePlaceholder, uniqueTestDirectory));
    String baseTargetPath = "/_dr_" + datasetNamePlaceholder + targetBasePathFormat;
    for (int i = 0; i < numFilesToLoad; i++) {
      directories.add(String.format(baseTargetPath, uniqueTestDirectory, "path", i));
    }
    directories.add(
        String.format("/_dr_%s/%s/diffpath", datasetNamePlaceholder, uniqueTestDirectory));
    directories.add(
        String.format(
            "/_dr_%s" + targetBasePathFormat,
            datasetNamePlaceholder,
            uniqueTestDirectory,
            "diffpath",
            "diff"));
    int expectedNum = 5 + numFilesToLoad;
    List<FireStoreDirectoryEntry> datasetDirectoryEntries =
        tableDirectoryDao.batchRetrieveByPath(
            tableServiceClient, collectionId, tableName, directories);
    assertThat(
        "Retrieved entries for all paths", datasetDirectoryEntries.size(), equalTo(expectedNum));
  }

  // TODO - add test case that tests out the cache mechanism

  // Test for snapshot file system
  // collectionId is the datasetId
  // snapshotId is obvious
  // - create dataset file system
  // - create subset snapshot file system
  // - do the compute and validate
  // Use binary for the sizes so each size combo will be unique
  @Test
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  public void testComputeSnapshot() throws Exception {

    // Make files that will be in the snapshot
    List<FireStoreDirectoryEntry> snapObjects = new ArrayList<>();
    String uniqueDir = String.format("/%s/adir", uniqueTestDirectory);
    snapObjects.add(makeFileObject(datasetId, uniqueDir + "/A1", 1));
    snapObjects.add(makeFileObject(datasetId, uniqueDir + "/bdir/B1", 2));
    snapObjects.add(makeFileObject(datasetId, uniqueDir + "/bdir/cdir/C1", 4));
    snapObjects.add(makeFileObject(datasetId, uniqueDir + "/bdir/cdir/C2", 8));

    // And some files that won't be in the snapshot
    List<FireStoreDirectoryEntry> dsetObjects = new ArrayList<>();
    dsetObjects.add(makeFileObject(datasetId, uniqueDir + "/bdir/B2", 16));
    dsetObjects.add(makeFileObject(datasetId, uniqueDir + "/A2", 32));

    // Make the dataset file system
    List<FireStoreDirectoryEntry> fileObjects = new ArrayList<>(snapObjects);
    fileObjects.addAll(dsetObjects);
    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : fileObjects) {
      tableDirectoryDao.createDirectoryEntry(
          tableServiceClient,
          datasetId,
          StorageTableName.DATASET.toTableName(datasetId),
          fireStoreDirectoryEntry);
    }

    // Make the snapshot file system
    Set<String> fileIdList = new HashSet<>();
    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : snapObjects) {
      fileIdList.add(fireStoreDirectoryEntry.getFileId());
    }
    tableDirectoryDao.addEntriesToSnapshot(
        tableServiceClient,
        tableServiceClient,
        datasetId,
        dataset.getName(),
        snapshotId,
        fileIdList,
        true);

    // Validate we cannot lookup dataset files in the snapshot
    for (FireStoreDirectoryEntry dsetObject : dsetObjects) {
      FireStoreDirectoryEntry snapObject =
          tableDirectoryDao.retrieveById(
              tableServiceClient,
              StorageTableName.SNAPSHOT.toTableName(snapshotId),
              dsetObject.getFileId());
      assertNull("object not found in snapshot", snapObject);
    }

    // Compute the size and checksums
    tableDao.snapshotCompute(snapshot, tableServiceClient, tableServiceClient);

    // Check the accumulated size on the root dir
    FireStoreDirectoryEntry snapObject =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient, snapshotId, StorageTableName.SNAPSHOT.toTableName(snapshotId), "/");
    assertNotNull("root exists", snapObject);
    assertThat("Total size is correct", snapObject.getSize(), equalTo(15L));
    assertThat(
        "The directory had its checksum calculated",
        snapObject.getChecksumMd5(),
        not(emptyOrNullString()));

    // Verify files
    fileIdList.forEach(
        fileId -> {
          FireStoreDirectoryEntry snapFileObject =
              tableDirectoryDao.retrieveById(
                  tableServiceClient, StorageTableName.SNAPSHOT.toTableName(snapshotId), fileId);
          assertNotNull("file exists", snapFileObject);
          assertThat(
              "the file had its checksum calculated",
              snapFileObject.getChecksumMd5(),
              not(emptyOrNullString()));
          assertThat(
              "the file has the proper directory", snapFileObject.getPath(), startsWith(uniqueDir));
        });
  }

  private FireStoreDirectoryEntry makeFileObject(UUID datasetId, String fullPath, long size) {

    String fileId = UUID.randomUUID().toString();

    FireStoreFile newFile =
        new FireStoreFile()
            .fileId(fileId)
            .mimeType("application/test")
            .description("test")
            .bucketResourceId("test")
            .fileCreatedDate(Instant.now().toString())
            .gspath(endpoint + "/" + fullPath)
            .checksumMd5(SnapshotCompute.computeMd5(fullPath))
            .userSpecifiedMd5(false)
            .size(size);

    tableFileDao.createFileMetadata(tableServiceClient, datasetId.toString(), newFile);

    return new FireStoreDirectoryEntry()
        .fileId(fileId)
        .isFileRef(true)
        .path(FileMetadataUtils.getDirectoryPath(fullPath))
        .name(FileMetadataUtils.getName(fullPath))
        .datasetId(datasetId.toString())
        .size(size)
        .checksumMd5(SnapshotCompute.computeMd5(fullPath));
  }
}
