package bio.terra.service.filedata.azure.tables;

import static bio.terra.service.common.azure.StorageTableName.DATASET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class TableDirectoryDaoConnectedTest {
  private static final Logger logger =
      LoggerFactory.getLogger(TableDirectoryDaoConnectedTest.class);
  private Dataset dataset;
  private UUID datasetId;
  private UUID snapshotId;
  private TableServiceClient tableServiceClient;
  private final List<String> directoryEntriesToCleanup = new ArrayList<>();
  private String snapshotTableName;

  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;
  @Autowired TableDirectoryDao tableDirectoryDao;
  @Autowired AzureUtils azureUtils;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    datasetId = UUID.randomUUID();
    dataset = new Dataset().id(datasetId).name(Names.randomizeName("datasetName"));
    snapshotId = UUID.randomUUID();
    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    testConfig.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://" + testConfig.getSourceStorageAccountName() + ".table.core.windows.net")
            .buildClient();
  }

  @After
  public void cleanup() throws Exception {
    // Should already be deleted
    for (String entry : directoryEntriesToCleanup) {
      try {
        tableDirectoryDao.deleteDirectoryEntry(
            tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(datasetId), entry);
      } catch (Exception ex) {
        logger.debug("Directory entry either already deleted or unable to delete {}", entry, ex);
      }
    }
    if (snapshotTableName != null) {
      try {
        TableClient tableClient = tableServiceClient.getTableClient(snapshotTableName);
        tableClient.deleteTable();
      } catch (Exception ex) {
        logger.error("Unable to delete table {}", snapshotTableName, ex);
      }
    }

    connectedOperations.teardown();
  }

  @Test
  public void testStoreTopDirectory() {
    tableDirectoryDao.storeTopDirectory(tableServiceClient, snapshotId, dataset.getName());

    snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    int count = getTableEntryCount(tableServiceClient, snapshotTableName);
    assertThat("Store top directory should add two entries to snapshot table.", count, equalTo(2));

    // get directories to confirm the correct ones are added
    List<String> directories = new ArrayList<>();
    directories.add("/");
    directories.add("/" + dataset.getName());

    List<FireStoreDirectoryEntry> datasetDirectoryEntries =
        tableDirectoryDao.batchRetrieveByPath(
            tableServiceClient, snapshotId, snapshotTableName, directories);
    datasetDirectoryEntries.forEach(d -> directoryEntriesToCleanup.add(d.getFileId()));
    assertThat("Retrieved entries for all paths", datasetDirectoryEntries.size(), equalTo(2));

    // Test that batchRetrieveByPath only returns unique entries
    List<String> nonUniqueDirectories = new ArrayList<>();
    nonUniqueDirectories.add("/" + dataset.getName());
    nonUniqueDirectories.add("/_dr_/" + dataset.getName());
    List<FireStoreDirectoryEntry> uniqueDatasetDirectoryEntries =
        tableDirectoryDao.batchRetrieveByPath(
            tableServiceClient, snapshotId, snapshotTableName, nonUniqueDirectories);
    assertThat("Only 1 unique path", uniqueDatasetDirectoryEntries.size(), equalTo(1));
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    // Test re-using same directory path, but for different files
    String sharedParentDir = UUID.randomUUID().toString();
    String sharedChildDir = UUID.randomUUID().toString();
    String sharedTargetPath = String.format("/%s/%s/", sharedParentDir, sharedChildDir);
    String fileName1 = "file1.json";
    var fileEntry1 = createStorageTableEntrySharedBasePath(sharedTargetPath, fileName1);
    String fileName2 = "file2.json";
    var fileEntry2 = createStorageTableEntrySharedBasePath(sharedTargetPath, fileName2);

    assertThat(
        "FireStoreDirectoryEntry should now exist",
        fileEntry1.getPath(),
        equalTo(FileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName1)));
    assertThat(
        "FireStoreDirectoryEntry should now exist",
        fileEntry2.getPath(),
        equalTo(FileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName2)));

    // Delete File 1's directory entry
    boolean deleteEntry =
        tableDirectoryDao.deleteDirectoryEntry(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            fileEntry1.getFileId());
    assertThat("Delete Entry 1", deleteEntry, equalTo(true));
    FireStoreDirectoryEntry shouldbeNull =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedTargetPath + fileName1);
    assertThat("File1 reference no longer exists", shouldbeNull, equalTo(null));

    FireStoreDirectoryEntry file2StillPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedTargetPath + fileName2);
    assertThat(
        "File2's directory still exists",
        file2StillPresent.getFileId(),
        equalTo(fileEntry2.getFileId()));

    // walk through sub directories make sure they still exist
    FireStoreDirectoryEntry childEntryStillPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedTargetPath);
    assertThat(
        String.format(
            "Shared subdirectory '%s' should still exist after single file delete",
            sharedTargetPath),
        childEntryStillPresent.getPath(),
        equalTo("/" + sharedParentDir));

    FireStoreDirectoryEntry parentEntryStillPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedParentDir);
    assertThat(
        String.format(
            "Shared subdirectory '/%s' should still exist after single file delete",
            sharedParentDir),
        parentEntryStillPresent.getPath(),
        equalTo("/"));

    FireStoreDirectoryEntry blankEntryStillPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(datasetId), "/");
    assertThat(
        "Shared subdirectory should still exist after single file delete",
        blankEntryStillPresent.getPath(),
        equalTo(""));

    // Delete the second file
    boolean deleteEntry2 =
        tableDirectoryDao.deleteDirectoryEntry(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            fileEntry2.getFileId());
    assertThat("Delete Entry 2", deleteEntry2, equalTo(true));
    FireStoreDirectoryEntry file2ShouldbeNull =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedTargetPath + fileName2);
    assertThat("File2 reference no longer exists", file2ShouldbeNull, equalTo(null));

    FireStoreDirectoryEntry testEntryNotPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedTargetPath);
    assertNull(
        String.format(
            "Shared subdirectory %s should not exist after remaining file delete",
            sharedTargetPath),
        testEntryNotPresent);
    FireStoreDirectoryEntry parentEntryNotPresent =
        tableDirectoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            sharedParentDir);
    assertNull(
        String.format(
            "Shared subdirectory '/%s' should not exist after remaining file delete",
            sharedParentDir),
        parentEntryNotPresent);

    // The root directory may still exist from concurrent test runs

  }

  @Test
  public void testEnumerateFileRefEntries() {
    String targetDirPath = String.format("/%s/", UUID.randomUUID());
    List<FireStoreDirectoryEntry> fileList =
        IntStream.range(0, 5)
            .mapToObj(
                i ->
                    createStorageTableEntrySharedBasePath(
                        targetDirPath, i + UUID.randomUUID().toString()))
            .toList();

    String collectionId = DATASET.toTableName(datasetId);
    List<FireStoreDirectoryEntry> files =
        tableDirectoryDao.enumerateFileRefEntries(tableServiceClient, collectionId, 0, 10);
    assertThat(files, equalTo(fileList));

    List<FireStoreDirectoryEntry> filesOffset =
        tableDirectoryDao.enumerateFileRefEntries(tableServiceClient, collectionId, 1, 10);
    assertThat(filesOffset, equalTo(fileList.subList(1, 5)));

    List<FireStoreDirectoryEntry> filesLimit =
        tableDirectoryDao.enumerateFileRefEntries(tableServiceClient, collectionId, 0, 2);
    assertThat(filesLimit, equalTo(fileList.subList(0, 2)));
  }

  private FireStoreDirectoryEntry createStorageTableEntrySharedBasePath(
      String sharedTargetPath, String fileName) {
    UUID fileId = UUID.randomUUID();
    String loadTag = Names.randomizeName("loadTag");

    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(fileId.toString())
            .isFileRef(true)
            .path(FileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName))
            .name(FileMetadataUtils.getName(sharedTargetPath + fileName))
            .datasetId(datasetId.toString())
            .loadTag(loadTag);
    tableDirectoryDao.createDirectoryEntry(
        tableServiceClient, datasetId, StorageTableName.DATASET.toTableName(datasetId), newEntry);
    directoryEntriesToCleanup.add(fileId.toString());

    // test that directory entry now exists
    return tableDirectoryDao.retrieveByPath(
        tableServiceClient,
        datasetId,
        StorageTableName.DATASET.toTableName(datasetId),
        sharedTargetPath + fileName);
  }

  private int getTableEntryCount(TableServiceClient tableServiceClient, String tableName) {
    ListEntitiesOptions options = new ListEntitiesOptions();
    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      return Iterables.size(entities);
    }
    return 0;
  }
}
