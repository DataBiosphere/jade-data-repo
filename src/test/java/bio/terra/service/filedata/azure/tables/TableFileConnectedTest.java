package bio.terra.service.filedata.azure.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.resourcemanager.AzureResourceManager;
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
public class TableFileConnectedTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired private AzureAuthService azureAuthService;
  @Autowired private TableFileDao tableFileDao;
  private TableServiceClient tableServiceClient;

  private static final String PARTITION_KEY = "partitionKey";
  private static final String FILE_ID = UUID.randomUUID().toString();
  private final TableEntity entity =
      new TableEntity(PARTITION_KEY, FILE_ID)
          .addProperty("fileId", FILE_ID)
          .addProperty("mimeType", "application/json")
          .addProperty("description", "A test entity")
          .addProperty("bucketResourceId", "bucketResourceId")
          .addProperty("loadTag", "loadTag")
          .addProperty("fileCreatedDate", "fileCreatedDate")
          .addProperty("gspath", "gspath")
          .addProperty("checksumCrc32c", "checksumCrc32c")
          .addProperty("checksumMd5", "checksumMd5")
          .addProperty("size", 1L);

  @Before
  public void setUp() {
    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    connectedTestConfiguration.getSourceStorageAccountName(),
                    getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://"
                    + connectedTestConfiguration.getSourceStorageAccountName()
                    + ".table.core.windows.net")
            .buildClient();
  }

  @Test
  public void testCreateDeleteEntry() {
    FireStoreFile fireStoreFile = FireStoreFile.fromTableEntity(entity);
    tableFileDao.createFileMetadata(tableServiceClient, fireStoreFile);
    FireStoreFile file = tableFileDao.retrieveFileMetadata(tableServiceClient, FILE_ID);
    assertEquals("The same file is retrieved", file, fireStoreFile);

    // Delete an entry
    boolean isDeleted = tableFileDao.deleteFileMetadata(tableServiceClient, FILE_ID);
    assertTrue("File record is deleted", isDeleted);
  }

  private String getSourceStorageAccountPrimarySharedKey() {
    AzureResourceManager client =
        azureResourceConfiguration.getClient(
            connectedTestConfiguration.getTargetTenantId(),
            connectedTestConfiguration.getTargetSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            connectedTestConfiguration.getTargetResourceGroupName(),
            connectedTestConfiguration.getSourceStorageAccountName())
        .getKeys()
        .iterator()
        .next()
        .value();
  }
}
