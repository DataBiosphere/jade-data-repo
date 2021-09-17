package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.ListTablesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
import com.azure.data.tables.models.TableServiceException;
import org.apache.commons.collections4.ListUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TableServiceClientUtils {
  private static final int MAX_FILTER_CLAUSES = 15;

  public static boolean tableHasEntries(
      TableServiceClient tableServiceClient, String tableName, ListEntitiesOptions options) {
    if (tableExists(tableServiceClient, tableName)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      options.setTop(1);
      PagedIterable<TableEntity> tableEntities = tableClient.listEntities(options, null, null);
      if (tableEntities.iterator().hasNext()) {
        return true;
      }
    }
    return false;
  }

  public static boolean tableHasEntries(TableServiceClient tableServiceClient, String tableName) {
    ListEntitiesOptions options = new ListEntitiesOptions();
    return tableHasEntries(tableServiceClient, tableName, options);
  }

  public static boolean tableExists(TableServiceClient tableServiceClient, String tableName) {
    ListTablesOptions options =
        new ListTablesOptions().setFilter(String.format("TableName eq '%s'", tableName));
    PagedIterable<TableItem> retrievedTableItems =
        tableServiceClient.listTables(options, null, null);
    return retrievedTableItems.iterator().hasNext();
  }

  public static List<TableEntity> batchRetrieveFiles(TableServiceClient tableServiceClient, String tableName, List<String> fileIdArray) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    String filter =
        fileIdArray.stream()
            //maybe wrap or cause in parenthesis
            .map(refId -> String.format("fileId eq '%s'", refId))
            .collect(Collectors.joining(" or "));
    ListEntitiesOptions options = new ListEntitiesOptions().setFilter(filter);
    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
      return tableClient.listEntities(options, null, null).stream().collect(Collectors.toList());
    }
    return null;
  }

  // TODO - add test
  private Optional<List<FireStoreDirectoryEntry>> batchRetrieveById(TableServiceClient tableServiceClient,
                                                          String tableName,
                                                          List<String> fileIds) {
    return ListUtils.partition(fileIds, MAX_FILTER_CLAUSES).stream()
        .flatMap(fileIdChunk -> {
          List<TableEntity> entities = batchRetrieveFiles(tableServiceClient, tableName, fileIdChunk);
          return Optional.of(entities.stream().map(entity -> {
            FireStoreDirectoryEntry directoryEntry = FireStoreDirectoryEntry.fromTableEntity(entity);
            if (!directoryEntry.getIsFileRef()) {
              throw new FileSystemExecutionException("Directories are not supported as references");
            }
            return directoryEntry;
          }).collect(Collectors.toList()));
        }).collect(Collectors.toList());
  }
}
