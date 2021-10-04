package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.Names;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.snapshot.Snapshot;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import java.util.ArrayList;
import java.util.List;
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
  private List<String> refIds;
  private String loadTag;
  private int numFilesToLoad;
  private String uniqueTestDirectory;
  private String targetBasePathFormat = "/%s/%s/file-%s.json";

  @Before
  public void setUp() {
    uniqueTestDirectory = UUID.randomUUID().toString().replace("-", "");
    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    connectedTestConfiguration.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://"
                    + connectedTestConfiguration.getSourceStorageAccountName()
                    + ".table.core.windows.net")
            .buildClient();
    datasetId = UUID.randomUUID();
    dataset = new Dataset().id(datasetId).name(Names.randomizeName("dataset"));
    refIds = new ArrayList<>();
    snapshotId = UUID.randomUUID();
    snapshot = new Snapshot().id(snapshotId);
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
    refIds.stream()
        .forEach(
            refId -> {
              boolean success =
                  tableDirectoryDao.deleteDirectoryEntry(
                      tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(), refId);
              logger.info("Delete {}: {}", refId, success);
            });

    // delete entire snapshot table
    tableDirectoryDao.deleteDirectoryEntriesFromCollection(
        tableServiceClient, StorageTableName.SNAPSHOT.toTableName(snapshotId));
  }

  @Test
  public void testAddFilesToSnapshot() {
    // First, make sure the directory entries exist in the dataset's storage table
    checkThatEntriesExist(datasetId, StorageTableName.DATASET.toTableName(), false);

    tableDao.addFilesToSnapshot(tableServiceClient, tableServiceClient, dataset, snapshot, refIds);

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
        tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(), newEntry);

    // test that directory entry now exists
    FireStoreDirectoryEntry de_after =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(), targetPath);
    assertThat("FireStoreDirectoryEntry should now exist", de_after, equalTo(newEntry));
  }

  private void checkThatEntriesExist(UUID collectionId, String tableName, boolean isSnapshot) {
    // For Snapshot lookups, we include the dataset name in the file path
    // This is excluded for datasets
    String datasetNamePlaceholder = "";
    if (isSnapshot) {
      datasetNamePlaceholder = "/" + dataset.getName();
    }

    List<String> directories = new ArrayList();
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
}
