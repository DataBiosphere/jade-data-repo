package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
import com.azure.data.tables.models.TableServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
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
public class TableDirectoryDaoTest {
  private static final String FULL_PATH = "/directory/file.json";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String PARTITION_KEY = DATASET_ID + " _dr_ directory";
  private static final String ROW_KEY = " _dr_ directory file.json";
  private static final String NONEXISTENT_PATH = "/directory/nonexistent.json";
  private static final String NONEXISTENT_ROW_KEY = " _dr_ directory nonexistent.json";
  private static final String FILE_ID = UUID.randomUUID().toString();
  private TableEntity entity;
  private FireStoreDirectoryEntry directoryEntry;

  @MockBean private AzureAuthService authService;
  @MockBean private TableServiceClient tableServiceClient;
  @MockBean private TableClient tableClient;
  @Autowired private TableDirectoryDao dao;

  @Before
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  public void setUp() {
    dao = spy(dao);
    when(authService.getTableServiceClient(any(), any(), any())).thenReturn(tableServiceClient);
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);

    // Mock table exists check
    PagedIterable<TableItem> mockTablesIterable = mock(PagedIterable.class);
    Iterator<TableItem> mockTableIterator = mock(Iterator.class);
    when(mockTableIterator.hasNext()).thenReturn(true, false);
    when(mockTablesIterable.iterator()).thenReturn(mockTableIterator);
    when(tableServiceClient.listTables(any(), any(), any())).thenReturn(mockTablesIterable);

    entity =
        new TableEntity(PARTITION_KEY, ROW_KEY)
            .addProperty(FireStoreDirectoryEntry.FILE_ID_FIELD_NAME, FILE_ID)
            .addProperty(FireStoreDirectoryEntry.IS_FILE_REF_FIELD_NAME, true)
            .addProperty(
                FireStoreDirectoryEntry.PATH_FIELD_NAME,
                FileMetadataUtils.getDirectoryPath(FULL_PATH))
            .addProperty(FireStoreDirectoryEntry.NAME_FIELD_NAME, "file.json")
            .addProperty(FireStoreDirectoryEntry.DATASET_ID_FIELD_NAME, DATASET_ID.toString())
            .addProperty(FireStoreDirectoryEntry.FILE_CREATED_DATE_FIELD_NAME, "fileCreatedDate")
            .addProperty(FireStoreDirectoryEntry.CHECKSUM_CRC32C_FIELD_NAME, "checksumCrc32c")
            .addProperty(FireStoreDirectoryEntry.CHECKSUM_MD5_FIELD_NAME, "checksumMd5")
            .addProperty(FireStoreDirectoryEntry.SIZE_FIELD_NAME, 1L);
    directoryEntry = FireStoreDirectoryEntry.fromTableEntity(entity);
  }

  @Test
  public void testRetrieveByPath() {
    when(tableClient.getEntity(PARTITION_KEY, ROW_KEY)).thenReturn(entity);
    FireStoreDirectoryEntry response =
        dao.retrieveByPath(
            tableServiceClient,
            DATASET_ID,
            StorageTableName.DATASET.toTableName(DATASET_ID),
            FULL_PATH);
    assertEquals("The same entry is returned", directoryEntry, response);

    when(tableClient.getEntity(PARTITION_KEY, NONEXISTENT_ROW_KEY))
        .thenThrow(TableServiceException.class);
    FireStoreDirectoryEntry nonExistentEntry =
        dao.retrieveByPath(
            tableServiceClient,
            DATASET_ID,
            StorageTableName.DATASET.toTableName(DATASET_ID),
            NONEXISTENT_PATH);
    assertNull("The entry does not exist", nonExistentEntry);
  }

  @Test
  public void testRetrieveByFileId() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(true, false);
    when(mockIterator.next()).thenReturn(entity);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);
    try (MockedStatic<TableServiceClientUtils> utils =
        Mockito.mockStatic(TableServiceClientUtils.class)) {
      utils
          .when(() -> TableServiceClientUtils.tableHasEntries(any(), any(), any()))
          .thenReturn(true);
      utils
          .when(() -> TableServiceClientUtils.tableHasSingleEntry(any(), any(), any()))
          .thenReturn(true);
      FireStoreDirectoryEntry response =
          dao.retrieveById(
              tableServiceClient, StorageTableName.DATASET.toTableName(DATASET_ID), FILE_ID);
      assertThat(
          "retrieveById returns the correct directory entry", response, equalTo(directoryEntry));
    }
  }

  @Test
  public void testRetrieveByFileIdNotFound() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(false);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    FireStoreDirectoryEntry response =
        dao.retrieveById(
            tableServiceClient, StorageTableName.DATASET.toTableName(DATASET_ID), "nonexistentId");
    assertNull("The entry does not exist", response);
  }

  @Test
  public void validateRefIdsFindsMissingRecords() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(false);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    String missingId = UUID.randomUUID().toString();
    List<String> refIds = List.of(missingId);
    List<String> response = dao.validateRefIds(tableServiceClient, DATASET_ID, refIds);
    assertEquals(response.get(0), missingId);
  }

  @Test
  public void testEnumerateDirectory() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Stream<TableEntity> mockStream = List.of(entity).stream();
    when(mockPagedIterable.stream()).thenReturn(mockStream);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    List<FireStoreDirectoryEntry> response =
        dao.enumerateDirectory(
            tableServiceClient, StorageTableName.DATASET.toTableName(DATASET_ID), FILE_ID);
    assertEquals(response.get(0), directoryEntry);
  }
}
