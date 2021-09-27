package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.Names;
import bio.terra.service.common.azure.StorageTableUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.snapshot.Snapshot;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired AzureUtils azureUtils;
  @Autowired TableDao tableDao;
  @Autowired TableFileDao tableFileDao;
  @Autowired TableDirectoryDao tableDirectoryDao;
  @Autowired FileMetadataUtils fileMetadataUtils;
  private TableServiceClient tableServiceClient;
  private UUID datasetId;
  private Dataset dataset;
  private UUID snapshotId;
  private Snapshot snapshot;
  private List<String> refIds;
  private String loadTag;
  private int numFilesToLoad;

  @Before
  public void setUp() {
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
    numFilesToLoad = 3;

    String baseTargetPath = "/test/path/file-%d.json";
    for (int i = 0; i < numFilesToLoad; i++) {
      String fileId = UUID.randomUUID().toString();
      refIds.add(fileId);
      String targetPath = String.format(baseTargetPath, i);
      createFileDirectoryEntry(fileId, targetPath);
    }
  }

  @Test
  public void testAddFilesToSnapshot() {
    // First, make sure the directory entries exist in the dataset's storage table
    checkThatEntriesExist(datasetId.toString(), StorageTableUtils.getDatasetTableName(), false);

    tableDao.addFilesToSnapshot(tableServiceClient, tableServiceClient, dataset, snapshot, refIds);

    // Now make sure that the same directory entries exist in the snapshot's storage table
    checkThatEntriesExist(
        snapshotId.toString(),
        StorageTableUtils.toTableName(
            snapshotId, StorageTableUtils.StorageTableNameSuffix.SNAPSHOT),
        true);
  }

  @Test
  public void testBatchRetrieve() {
    List<TableEntity> entities =
        TableServiceClientUtils.batchRetrieveFiles(tableServiceClient, refIds);
    assertThat(
        "One file id returned from batchRetreiveFiles", entities.size(), equalTo(numFilesToLoad));
    // assertThat("Right file is returned", entities.get(0).getPartitionKey(), equalTo(1));
  }

  private void createFileDirectoryEntry(String fileId, String targetPath) {
    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(fileId)
            .isFileRef(true)
            .path(fileMetadataUtils.getDirectoryPath(targetPath))
            .name(fileMetadataUtils.getName(targetPath))
            .datasetId(datasetId.toString())
            .loadTag(loadTag);
    tableDirectoryDao.createDirectoryEntry(
        tableServiceClient,
        datasetId.toString(),
        StorageTableUtils.getDatasetTableName(),
        newEntry);

    // test that directory entry now exists
    FireStoreDirectoryEntry de_after =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId.toString(),
            StorageTableUtils.getDatasetTableName(),
            targetPath);
    assertThat("FireStoreDirectoryEntry should now exist", de_after, equalTo(newEntry));
  }

  private void checkThatEntriesExist(String collectionId, String tableName, boolean isSnapshot) {
    // For Snapshot lookups, we include the dataset name in the file path
    // This is excluded for datasets
    String datasetNamePlaceholder = "";
    if (isSnapshot) {
      datasetNamePlaceholder = dataset.getName() + "/";
    }

    List<String> directories = new ArrayList();
    directories.add("/");
    directories.add("/_dr_");
    directories.add("/_dr_/" + datasetNamePlaceholder + "test");
    directories.add("/_dr_/" + datasetNamePlaceholder + "test/path");
    String baseTargetPath = "/_dr_/" + datasetNamePlaceholder + "test/path/file-%d.json";
    for (int i = 0; i < numFilesToLoad; i++) {
      directories.add(String.format(baseTargetPath, i));
    }
    int expectedNum = 4 + numFilesToLoad;
    List<FireStoreDirectoryEntry> datasetDirectoryEntries =
        tableDirectoryDao.batchRetrieveByPath(
            tableServiceClient, collectionId, tableName, directories);
    assertThat(
        "Retrieved entries for all paths", datasetDirectoryEntries.size(), equalTo(expectedNum));
  }

  // TODO - add test case that tests out the cache mechanism
  // TODO - test multiple files in snapshot - (A) With shared file paths and (B) with different file
  // paths
}
