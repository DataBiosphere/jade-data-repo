package bio.terra.service.filedata.azure.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.resourcemanagement.azure.*;
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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class TableFileDaoTest {
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
          .addProperty("gspath", "gsPath")
          .addProperty("checksumCrc32c", "checksumCrc32c")
          .addProperty("checksumMd5", "checksumMd5")
          .addProperty("size", 1L);

  @MockBean private AzureAuthService authService;
  @MockBean private TableServiceClient tableServiceClient;
  @MockBean private TableClient tableClient;
  @MockBean private FireStoreUtils fireStoreUtils;
  @MockBean private ExecutorService executor;
  @Autowired private TableFileDao dao;

  @Before
  public void setUp() throws Exception {
    dao = spy(dao);
    when(authService.getTableServiceClient(any(), any())).thenReturn(tableServiceClient);
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
    when(tableClient.getEntity(PARTITION_KEY, FILE_ID)).thenReturn(entity);
    when(tableClient.getEntity(PARTITION_KEY, "nonexistentFile"))
        .thenThrow(TableServiceException.class);
  }

  @Test
  public void testRetrieveFileMetadata() {
    FireStoreFile fileMetadata = dao.retrieveFileMetadata(tableServiceClient, FILE_ID);
    FireStoreFile expected = FireStoreFile.fromTableEntity(entity);
    assertEquals("The same object is returned", fileMetadata, expected);
  }

  @Test
  public void testDeleteFileMetadata() {
    boolean exists = dao.deleteFileMetadata(tableServiceClient, FILE_ID);
    assertTrue("Existing row is deleted", exists);
    boolean result = dao.deleteFileMetadata(tableServiceClient, "nonexistentFile");
    assertFalse("Non-existent row is not deleted", result);
  }

  @Test
  public void testBatchRetrieveFileMetadata() {
    FireStoreDirectoryEntry fsDirectoryEntry = new FireStoreDirectoryEntry().fileId(FILE_ID);
    List<FireStoreDirectoryEntry> directoryEntries = List.of(fsDirectoryEntry);
    List<FireStoreFile> expectedFiles = List.of(FireStoreFile.fromTableEntity(entity));
    List<FireStoreFile> files = dao.batchRetrieveFileMetadata(tableServiceClient, directoryEntries);
    assertEquals(
        "A file record is found for each directory entry", files.size(), expectedFiles.size());
    assertEquals("The same object is returned", files.get(0), expectedFiles.get(0));
  }
}
