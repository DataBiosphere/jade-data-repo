package bio.terra.service.filedata.azure.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class TableFileDaoTest {
  private static final String PARTITION_KEY = "partitionKey";
  private static final String DATASET_ID = UUID.randomUUID().toString();
  private static final String FILE_ID = UUID.randomUUID().toString();
  private final TableEntity entity = createTableEntity(true);

  @MockBean private AzureAuthService authService;
  @MockBean private TableServiceClient tableServiceClient;
  @MockBean private TableClient tableClient;
  @MockBean private FireStoreUtils fireStoreUtils;

  @MockBean
  @Qualifier("performanceThreadpool")
  private ExecutorService executor;

  @Autowired private TableFileDao dao;

  @Before
  public void setUp() throws Exception {
    dao = spy(dao);
    when(authService.getTableServiceClient(any(), any(), any())).thenReturn(tableServiceClient);
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
    when(tableClient.getEntity(PARTITION_KEY, FILE_ID)).thenReturn(entity);
    when(tableClient.getEntity(PARTITION_KEY, "nonexistentFile"))
        .thenThrow(TableServiceException.class);
  }

  @Test
  public void testRetrieveFileMetadata() {
    FireStoreFile fileMetadata = dao.retrieveFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    FireStoreFile expected = FireStoreFile.fromTableEntity(entity);
    assertEquals("The same object is returned", fileMetadata, expected);
  }

  @Test
  public void testFromTableEntityMissingMimeType() {
    TableEntity entity = createTableEntity(false);
    FireStoreFile expected = FireStoreFile.fromTableEntity(entity);
    assertNull("Entity mimeType does not throw NPE", expected.getMimeType());
  }

  @Test
  public void testDeleteFileMetadata() {
    boolean exists = dao.deleteFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    assertTrue("Existing row is deleted", exists);
    boolean result = dao.deleteFileMetadata(tableServiceClient, DATASET_ID, "nonexistentFile");
    assertFalse("Non-existent row is not deleted", result);
  }

  @Test
  public void testBatchRetrieveFileMetadata() {
    FireStoreDirectoryEntry fsDirectoryEntry = new FireStoreDirectoryEntry().fileId(FILE_ID);
    List<FireStoreDirectoryEntry> directoryEntries = List.of(fsDirectoryEntry);
    List<FireStoreFile> expectedFiles = List.of(FireStoreFile.fromTableEntity(entity));
    List<FireStoreFile> files =
        dao.batchRetrieveFileMetadata(tableServiceClient, DATASET_ID, directoryEntries);
    assertEquals(
        "A file record is found for each directory entry", files.size(), expectedFiles.size());
    assertEquals("The same object is returned", files.get(0), expectedFiles.get(0));
  }

  private TableEntity createTableEntity(boolean hasMimeType) {
    TableEntity entity =
        new TableEntity(PARTITION_KEY, FILE_ID)
            .addProperty(FireStoreFile.FILE_ID_FIELD_NAME, FILE_ID)
            .addProperty(FireStoreFile.DESCRIPTION_FIELD_NAME, "A test entity")
            .addProperty(FireStoreFile.BUCKET_RESOURCE_ID_FIELD_NAME, "bucketResourceId")
            .addProperty(FireStoreFile.LOAD_TAG_FIELD_NAME, "loadTag")
            .addProperty(FireStoreFile.FILE_CREATED_DATE_FIELD_NAME, "fileCreatedDate")
            .addProperty(FireStoreFile.GS_PATH_FIELD_NAME, "gsPath")
            .addProperty(FireStoreFile.CHECKSUM_CRC32C_FIELD_NAME, "checksumCrc32c")
            .addProperty(FireStoreFile.CHECKSUM_MD5_FIELD_NAME, "checksumMd5")
            .addProperty(FireStoreFile.SIZE_FIELD_NAME, 1L);
    if (hasMimeType) {
      entity.addProperty(FireStoreFile.MIME_TYPE_FIELD_NAME, "application/json");
    }
    return entity;
  }
}
