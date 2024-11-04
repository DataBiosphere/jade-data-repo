package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
import com.azure.data.tables.models.TableServiceException;
import java.util.Iterator;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Tag(Unit.TAG)
class TableServiceClientUtilsTest {

  @Mock TableServiceClient tableServiceClient;
  @Mock TableClient tableClient;

  @BeforeEach
  void setUp() {
    // mock table exists check
    mockTableExists(true);

    // get table client
    when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tableHasEntries(boolean hasEntries) {
    mockTableHasEntries(hasEntries);

    assertThat(
        TableServiceClientUtils.tableHasEntries(tableServiceClient, "tableName", null),
        equalTo(hasEntries));
  }

  @Test
  void tableHasEntriesCatchThrownException() {
    when(tableClient.listEntities(any(), any(), any()))
        .thenThrow(new TableServiceException("error", mock(HttpResponse.class)));

    assertFalse(TableServiceClientUtils.tableHasEntries(tableServiceClient, "tableName", null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tableExists(boolean tableExists) {
    mockTableExists(tableExists);

    assertThat(
        TableServiceClientUtils.tableExists(tableServiceClient, "tableName"), equalTo(tableExists));
  }

  @Test
  void tableExistsCatchThrownException() {
    when(tableServiceClient.listTables(any(), any(), any()))
        .thenThrow(new TableServiceException("error", mock(HttpResponse.class)));

    assertFalse(TableServiceClientUtils.tableExists(tableServiceClient, "tableName"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void filterTable(boolean hasEntries) {
    mockTableHasEntries(true);

    // Mock listing entities with filter
    var filter = "exampleParameter eq '1'";
    TableEntity fireStoreDependencyEntity = new TableEntity("partitionKey", "rowKey");
    PagedIterable<TableEntity> mockPagedIterable2 = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator2 = mock(Iterator.class);
    when(mockIterator2.hasNext()).thenReturn(hasEntries, false);
    when(mockIterator2.next()).thenReturn(fireStoreDependencyEntity);
    when(mockPagedIterable2.iterator()).thenReturn(mockIterator2);
    when(mockPagedIterable2.stream()).thenReturn(Stream.of(fireStoreDependencyEntity));
    // only match for listing entities with filter
    ArgumentMatcher<ListEntitiesOptions> matcher =
        options -> options.getFilter() != null && options.getFilter().contains(filter);
    when(tableClient.listEntities(argThat(matcher), any(), any())).thenReturn(mockPagedIterable2);

    assertThat(
        TableServiceClientUtils.filterTable(tableServiceClient, "tableName", filter),
        hasSize(hasEntries ? 1 : 0));
  }

  @Test
  void filterTableCatchThrownException() {
    mockTableHasEntries(true);

    var filter = "exampleParameter eq '1'";
    // only match for listing entities with filter
    ArgumentMatcher<ListEntitiesOptions> matcher =
        options -> options.getFilter() != null && options.getFilter().contains(filter);
    when(tableClient.listEntities(argThat(matcher), any(), any()))
        .thenThrow(new TableServiceException("error", mock(HttpResponse.class)));

    assertThat(
        TableServiceClientUtils.filterTable(tableServiceClient, "tableName", filter), hasSize(0));
  }

  private void mockTableExists(boolean shouldExist) {
    PagedIterable<TableItem> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableItem> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(shouldExist);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableServiceClient.listTables(any(), any(), any())).thenReturn(mockPagedIterable);
  }

  private void mockTableHasEntries(boolean shouldHaveEntries) {
    PagedIterable<TableEntity> mockPagedIterable = mock(PagedIterable.class);
    Iterator<TableEntity> mockIterator = mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(shouldHaveEntries);
    when(mockPagedIterable.iterator()).thenReturn(mockIterator);
    when(tableClient.listEntities(any(), any(), any())).thenReturn(mockPagedIterable);
  }
}
