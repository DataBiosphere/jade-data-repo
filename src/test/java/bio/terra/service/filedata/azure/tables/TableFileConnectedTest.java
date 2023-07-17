package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
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
public class TableFileConnectedTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired private TableFileDao tableFileDao;
  @Autowired AzureUtils azureUtils;
  private TableServiceClient tableServiceClient;

  private static final String PARTITION_KEY = "partitionKey";
  private static final String DATASET_ID = UUID.randomUUID().toString();
  private static final String FILE_ID = UUID.randomUUID().toString();
  private final TableEntity entity =
      new TableEntity(PARTITION_KEY, FILE_ID)
          .addProperty(FireStoreFile.FILE_ID_FIELD_NAME, FILE_ID)
          .addProperty(FireStoreFile.MIME_TYPE_FIELD_NAME, "application/json")
          .addProperty(FireStoreFile.DESCRIPTION_FIELD_NAME, "A test entity")
          .addProperty(FireStoreFile.BUCKET_RESOURCE_ID_FIELD_NAME, "bucketResourceId")
          .addProperty(FireStoreFile.LOAD_TAG_FIELD_NAME, "loadTag")
          .addProperty(FireStoreFile.FILE_CREATED_DATE_FIELD_NAME, "fileCreatedDate")
          .addProperty(FireStoreFile.GS_PATH_FIELD_NAME, "gspath")
          .addProperty(FireStoreFile.CHECKSUM_CRC32C_FIELD_NAME, "checksumCrc32c")
          .addProperty(FireStoreFile.CHECKSUM_MD5_FIELD_NAME, "checksumMd5")
          .addProperty(FireStoreFile.SIZE_FIELD_NAME, 1L);

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
  public void testCreateDeleteEntry() {
    FireStoreFile fireStoreFile = FireStoreFile.fromTableEntity(entity);
    tableFileDao.createFileMetadata(tableServiceClient, DATASET_ID, fireStoreFile);
    FireStoreFile file = tableFileDao.retrieveFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    assertEquals("The same file is retrieved", file, fireStoreFile);

    // Delete an entry
    boolean isDeleted = tableFileDao.deleteFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    assertTrue("File record is deleted", isDeleted);

    // Try to delete the entry again
    boolean isNotDeleted = tableFileDao.deleteFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    assertFalse("File record was already deleted", isNotDeleted);
  }

  @Test
  public void testListEntry() {
    List<FireStoreFile> fileList = IntStream.range(0, 5).boxed().map(this::makeFile).toList();
    for (FireStoreFile fsFile : fileList) {
      tableFileDao.createFileMetadata(tableServiceClient, DATASET_ID, fsFile);
    }
    List<FireStoreFile> files =
        tableFileDao.listFileMetadata(tableServiceClient, DATASET_ID, 0, 10);
    assertThat(files, equalTo(fileList));

    List<FireStoreFile> filesOffset =
        tableFileDao.listFileMetadata(tableServiceClient, DATASET_ID, 1, 10);
    assertThat(filesOffset.size(), equalTo(4));
    //    assertThat(filesOffset, equalTo(fileList.subList(1, 5)));

    List<FireStoreFile> filesLimit =
        tableFileDao.listFileMetadata(tableServiceClient, DATASET_ID, 0, 2);
    assertThat(filesLimit.size(), equalTo(2));
    //    assertThat(filesLimit, equalTo(fileList.subList(0, 3)));
  }

  private FireStoreFile makeFile(int index) {
    String fileId = UUID.randomUUID().toString();
    TableEntity entity =
        new TableEntity(PARTITION_KEY, index + fileId)
            .addProperty(FireStoreFile.FILE_ID_FIELD_NAME, index + fileId);
    return FireStoreFile.fromTableEntity(entity);
  }
}
