package bio.terra.service.filedata.azure.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
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
public class TableDirectoryDaoTest {
  private static final String FULL_PATH = "/directory/file.json";
  private static final String DATASET_ID = UUID.randomUUID().toString();
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
  @Autowired private FileMetadataUtils fileMetadataUtils;
  @Autowired private TableDirectoryDao dao;

  @Before
  public void setUp() {
    dao = spy(dao);
    when(authService.getTableServiceClient(any(), any())).thenReturn(tableServiceClient);
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
    entity =
        new TableEntity(PARTITION_KEY, ROW_KEY)
            .addProperty("fileId", FILE_ID)
            .addProperty("isFileRef", true)
            .addProperty("path", fileMetadataUtils.getDirectoryPath(FULL_PATH))
            .addProperty("name", "file.json")
            .addProperty("datasetId", DATASET_ID)
            .addProperty("fileCreatedDate", "fileCreatedDate")
            .addProperty("checksumCrc32c", "checksumCrc32c")
            .addProperty("checksumMd5", "checksumMd5")
            .addProperty("size", 1L);
    directoryEntry = FireStoreDirectoryEntry.fromTableEntity(entity);
  }

  @Test
  public void testRetrieveByPath() {
    when(tableClient.getEntity(PARTITION_KEY, ROW_KEY)).thenReturn(entity);
    FireStoreDirectoryEntry response =
        dao.retrieveByPath(tableServiceClient, DATASET_ID, FULL_PATH);
    assertEquals("The same entry is returned", directoryEntry, response);

    when(tableClient.getEntity(PARTITION_KEY, NONEXISTENT_ROW_KEY))
        .thenThrow(TableServiceException.class);
    FireStoreDirectoryEntry nonExistentEntry =
        dao.retrieveByPath(tableServiceClient, DATASET_ID, NONEXISTENT_PATH);
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

    FireStoreDirectoryEntry response = dao.retrieveById(tableServiceClient, FILE_ID);
    assertEquals(response, directoryEntry);
  }

  @Test
  public void testRetrieveByFileIdNotFound() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(false);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    FireStoreDirectoryEntry response = dao.retrieveById(tableServiceClient, "nonexistentId");
    assertNull("The entry does not exist", response);
  }

  @Test
  public void validateRefIdsFindsMissingRecords() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(false);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    List<String> refIds = List.of("missingId");
    List<String> response = dao.validateRefIds(tableServiceClient, refIds);
    assertEquals(response.get(0), "missingId");
  }

  @Test
  public void testEnumerateDirectory() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Stream<TableEntity> mockStream = List.of(entity).stream();
    when(mockPagedIterable.stream()).thenReturn(mockStream);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    List<FireStoreDirectoryEntry> response = dao.enumerateDirectory(tableServiceClient, FILE_ID);
    assertEquals(response.get(0), directoryEntry);
  }
}
