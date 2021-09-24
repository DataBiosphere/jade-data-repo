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
  private static final String FILE_ID = UUID.randomUUID().toString();
  private String targetPath;

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
    refIds = List.of(FILE_ID);
    snapshotId = UUID.randomUUID();
    snapshot = new Snapshot().id(snapshotId);
    targetPath = "/test/path/file.json";

    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(FILE_ID)
            .isFileRef(true)
            .path(fileMetadataUtils.getDirectoryPath(targetPath))
            .name(fileMetadataUtils.getName(targetPath))
            .datasetId(datasetId.toString())
            .loadTag(Names.randomizeName("loadTag"));
    tableDirectoryDao.createDirectoryEntry(
        tableServiceClient, StorageTableUtils.getDatasetTableName(), newEntry);

    // test that directory entry now exists
    FireStoreDirectoryEntry de_after =
        tableDirectoryDao.retrieveByPath(tableServiceClient, datasetId.toString(), targetPath);
    assertThat("FireStoreDirectoryEntry should now exist", de_after, equalTo(newEntry));
  }

  @Test
  public void testAddFilesToSnapshot() {
    tableDao.addFilesToSnapshot(tableServiceClient, tableServiceClient, dataset, snapshot, refIds);

    // check if exist
    List<String> directories = List.of("/_dr_/test", "/_dr_/test/path");
    List<FireStoreDirectoryEntry> datasetDirectoryEntries =
        tableDirectoryDao.batchRetrieveByPath(
            tableServiceClient, datasetId.toString(), directories);
    assertThat(
        "Retrieved two entries for the two paths", datasetDirectoryEntries.size(), equalTo(2));
  }

  @Test
  public void testBatchRetrieve() {
    List<TableEntity> entities =
        TableServiceClientUtils.batchRetrieveFiles(tableServiceClient, refIds);
    assertThat("One file id returned from batchRetreiveFiles", entities.size(), equalTo(1));
    // assertThat("Right file is returned", entities.get(0).getPartitionKey(), equalTo(1));
  }
}
