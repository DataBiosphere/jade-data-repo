package bio.terra.service.filedata.azure.tables;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.common.azure.StorageTableName;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import java.util.Set;
import java.util.UUID;
import org.junit.Assert;
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
@EmbeddedDatabaseTest
public class TableDependencyConnectedTest {

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired private TableDependencyDao dependencyDao;
  @Autowired AzureUtils azureUtils;
  private TableServiceClient tableServiceClient;

  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID2 = UUID.randomUUID();
  private static final String FILE_ID = UUID.randomUUID().toString();
  private static final Set<String> REF_IDS = Set.of(FILE_ID);

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
  }

  @Test
  public void testCreateDeleteDependencyEntries() {
    String tableName = StorageTableName.DEPENDENCIES.toTableName(DATASET_ID);
    TableClient tableClient = tableServiceClient.getTableClient(tableName);

    // Add snapshot file dependency
    dependencyDao.storeSnapshotFileDependencies(
        tableServiceClient, DATASET_ID, SNAPSHOT_ID, REF_IDS);
    TableEntity createdEntity = tableClient.getEntity(SNAPSHOT_ID.toString(), FILE_ID);
    azureUtils.assertEntityCorrect(createdEntity, SNAPSHOT_ID, FILE_ID, 1L);

    // Add same file in a different snapshot
    dependencyDao.storeSnapshotFileDependencies(
        tableServiceClient, DATASET_ID, SNAPSHOT_ID2, REF_IDS);

    // Deleting the SNAPSHOT_ID dependencies does not affect other snapshot records
    dependencyDao.deleteSnapshotFileDependencies(tableServiceClient, DATASET_ID, SNAPSHOT_ID);
    assertThrows(
        TableServiceException.class, () -> tableClient.getEntity(SNAPSHOT_ID.toString(), FILE_ID));
    assertNotNull(tableClient.getEntity(SNAPSHOT_ID2.toString(), FILE_ID));

    dependencyDao.deleteSnapshotFileDependencies(tableServiceClient, DATASET_ID, SNAPSHOT_ID2);
    assertThrows(
        TableServiceException.class, () -> tableClient.getEntity(SNAPSHOT_ID2.toString(), FILE_ID));
  }

  @Test
  public void testDatasetHasSnapshotReference() {
    Assert.assertFalse(dependencyDao.datasetHasSnapshotReference(tableServiceClient, DATASET_ID));
    String tableName = StorageTableName.DEPENDENCIES.toTableName(DATASET_ID);
    TableClient tableClient = tableServiceClient.getTableClient(tableName);

    // Add snapshot file dependency
    dependencyDao.storeSnapshotFileDependencies(
        tableServiceClient, DATASET_ID, SNAPSHOT_ID, REF_IDS);
    TableEntity createdEntity = tableClient.getEntity(SNAPSHOT_ID.toString(), FILE_ID);
    azureUtils.assertEntityCorrect(createdEntity, SNAPSHOT_ID, FILE_ID, 1L);

    Assert.assertTrue(dependencyDao.datasetHasSnapshotReference(tableServiceClient, DATASET_ID));
  }
}
