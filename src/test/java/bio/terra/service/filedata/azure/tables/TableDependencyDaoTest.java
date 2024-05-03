package bio.terra.service.filedata.azure.tables;

import static bio.terra.service.filedata.google.firestore.FireStoreDependency.FILE_ID_FIELD_NAME;
import static bio.terra.service.filedata.google.firestore.FireStoreDependency.REF_COUNT_FIELD_NAME;
import static bio.terra.service.filedata.google.firestore.FireStoreDependency.SNAPSHOT_ID_FIELD_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
public class TableDependencyDaoTest {
  @MockBean private AzureAuthService authService;
  @MockBean private TableServiceClient tableServiceClient;
  @MockBean private TableClient tableClient;
  @Autowired private TableDependencyDao dao;

  @Captor private ArgumentCaptor<ListEntitiesOptions> queryOptionsCaptor;

  @Before
  public void setUp() {
    dao = spy(dao);
    when(authService.getTableServiceClient(any(), any(), any())).thenReturn(tableServiceClient);
    when(authService.getTableServiceClient(any())).thenReturn(tableServiceClient);
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
    dao.storeSnapshotFileDependencies(tableServiceClient, datasetId, snapshotId, Set.of(refId));
    verify(tableClient, times(1)).submitTransaction(any());
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

    dao.storeSnapshotFileDependencies(tableServiceClient, datasetId, snapshotId, Set.of(refId));
    verify(tableClient, times(0)).upsertEntity(any());
    verify(tableClient, times(0)).submitTransaction(any());
  }

  @Test
  public void testReadSnapshotFileDependencies() {
    UUID datasetId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();
    String refId1 = UUID.randomUUID().toString();
    String refId2 = UUID.randomUUID().toString();
    TableEntity fireStoreDependencyEntity1 =
        new TableEntity(datasetId.toString(), refId1)
            .addProperty(SNAPSHOT_ID_FIELD_NAME, snapshotId)
            .addProperty(FILE_ID_FIELD_NAME, refId1)
            .addProperty(REF_COUNT_FIELD_NAME, 1L);

    TableEntity fireStoreDependencyEntity2 =
        new TableEntity(datasetId.toString(), refId2)
            .addProperty(SNAPSHOT_ID_FIELD_NAME, snapshotId)
            .addProperty(FILE_ID_FIELD_NAME, refId2)
            .addProperty(REF_COUNT_FIELD_NAME, 1L);

    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(true, false);
    when(mockIterator.next())
        .thenReturn(fireStoreDependencyEntity1)
        .thenReturn(fireStoreDependencyEntity2);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(mockPagedIterable.stream())
        .thenReturn(Stream.of(fireStoreDependencyEntity1, fireStoreDependencyEntity2));
    when(tableClient.listEntities(queryOptionsCaptor.capture(), any(), any()))
        .thenReturn(mockPagedIterable);

    assertThat(
        "proper ids are returned",
        dao.getDatasetSnapshotFileIds(
            tableServiceClient, new Dataset().id(datasetId), snapshotId.toString()),
        containsInAnyOrder(refId1, refId2));

    assertThat(
        "right filter was used",
        queryOptionsCaptor.getValue().getFilter(),
        equalTo("snapshotId eq '%s'".formatted(snapshotId)));
  }
}
