package bio.terra.service.filedata.azure.tables;

import static bio.terra.service.filedata.google.firestore.FireStoreDependency.FILE_ID_FIELD_NAME;
import static bio.terra.service.filedata.google.firestore.FireStoreDependency.REF_COUNT_FIELD_NAME;
import static bio.terra.service.filedata.google.firestore.FireStoreDependency.SNAPSHOT_ID_FIELD_NAME;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
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
public class TableDependencyDaoTest {
  @MockBean private AzureAuthService authService;
  @MockBean private TableServiceClient tableServiceClient;
  @MockBean private TableClient tableClient;
  @Autowired private TableDependencyDao dao;

  @Before
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
  }

  @Test
  public void testAddSnapshotFileDependencies() {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(false);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    UUID datasetId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();
    String refId = UUID.randomUUID().toString();
    dao.storeSnapshotFileDependencies(tableServiceClient, datasetId, snapshotId, List.of(refId));
    verify(tableClient, times(1)).upsertEntity(any());
  }

  @Test
  public void testAddSnapshotFileDependenciesExisting() {
    UUID datasetId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();
    String refId = UUID.randomUUID().toString();
    TableEntity fireStoreDependencyEntity =
        new TableEntity(datasetId.toString(), refId)
            .addProperty(SNAPSHOT_ID_FIELD_NAME, snapshotId)
            .addProperty(FILE_ID_FIELD_NAME, refId)
            .addProperty(REF_COUNT_FIELD_NAME, 1L);

    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(true, false);
    when(mockIterator.next()).thenReturn(fireStoreDependencyEntity);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(mockPagedIterable.stream()).thenReturn(Stream.of(fireStoreDependencyEntity));
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);

    dao.storeSnapshotFileDependencies(tableServiceClient, datasetId, snapshotId, List.of(refId));
    verify(tableClient, times(0)).upsertEntity(any());
  }
}
