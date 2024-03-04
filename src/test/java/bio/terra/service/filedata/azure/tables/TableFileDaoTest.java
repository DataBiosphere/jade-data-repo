package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class TableFileDaoTest {
  private static final String PARTITION_KEY = "partitionKey";
  private static final String FILE_ID = UUID.randomUUID().toString();
  private static final String DATASET_ID = UUID.randomUUID().toString();

  @Mock private TableServiceClient tableServiceClient;
  @Mock private TableClient tableClient;
  private final TableFileDao dao = new TableFileDao(Runnable::run);
  private final TableEntity entity =
      new TableEntity(PARTITION_KEY, FILE_ID)
          .addProperty(FireStoreFile.FILE_ID_FIELD_NAME, FILE_ID)
          .addProperty(FireStoreFile.MIME_TYPE_FIELD_NAME, "application/json")
          .addProperty(FireStoreFile.DESCRIPTION_FIELD_NAME, "A test entity")
          .addProperty(FireStoreFile.BUCKET_RESOURCE_ID_FIELD_NAME, "bucketResourceId")
          .addProperty(FireStoreFile.LOAD_TAG_FIELD_NAME, "loadTag")
          .addProperty(FireStoreFile.FILE_CREATED_DATE_FIELD_NAME, "fileCreatedDate")
          .addProperty(FireStoreFile.GS_PATH_FIELD_NAME, "gsPath")
          .addProperty(FireStoreFile.CHECKSUM_CRC32C_FIELD_NAME, "checksumCrc32c")
          .addProperty(FireStoreFile.CHECKSUM_MD5_FIELD_NAME, "checksumMd5")
          .addProperty(FireStoreFile.SIZE_FIELD_NAME, 1L);

  @BeforeEach
  void setUp() {
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
    when(tableClient.getEntity(PARTITION_KEY, FILE_ID)).thenReturn(entity);
  }

  @Test
  void testRetrieveFileMetadata() {
    FireStoreFile fileMetadata = dao.retrieveFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    FireStoreFile expected = FireStoreFile.fromTableEntity(entity);
    assertEquals(fileMetadata, expected, "The same object is returned");
  }

  @Test
  void testDeleteFileMetadata() {
    boolean exists = dao.deleteFileMetadata(tableServiceClient, DATASET_ID, FILE_ID);
    assertTrue(exists, "Existing row is deleted");

    when(tableClient.getEntity(PARTITION_KEY, "nonexistentFile"))
        .thenThrow(TableServiceException.class);
    boolean result = dao.deleteFileMetadata(tableServiceClient, DATASET_ID, "nonexistentFile");
    assertFalse(result, "Non-existent row is not deleted");
  }

  @Test
  void testBatchRetrieveFileMetadata() {
    FireStoreDirectoryEntry fsDirectoryEntry = new FireStoreDirectoryEntry().fileId(FILE_ID);
    List<FireStoreDirectoryEntry> directoryEntries = List.of(fsDirectoryEntry);
    List<FireStoreFile> expectedFiles = List.of(FireStoreFile.fromTableEntity(entity));
    List<FireStoreFile> files =
        dao.batchRetrieveFileMetadata(tableServiceClient, DATASET_ID, directoryEntries);
    assertThat(files, equalTo(expectedFiles));
  }
}
