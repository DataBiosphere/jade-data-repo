package bio.terra.service.filedata.azure.tables;

import bio.terra.service.common.azure.StorageTableUtils;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.ListTablesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TableServiceClientUtils {

  public static boolean tableHasEntries(
      TableServiceClient tableServiceClient, String tableName, ListEntitiesOptions options) {
    if (options == null) {
      options = new ListEntitiesOptions();
    }
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

  public static List<TableEntity> filterTable(
      TableServiceClient tableServiceClient, String tableName, String filter) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    ListEntitiesOptions options = new ListEntitiesOptions().setFilter(filter);
    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
      return tableClient.listEntities(options, null, null).stream().collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public static List<TableEntity> batchRetrieveFiles(
      TableServiceClient tableServiceClient, List<String> fileIdArray) {
    String filter =
        fileIdArray.stream()
            // maybe wrap or cause in parenthesis
            .map(refId -> String.format("fileId eq '%s'", refId))
            .collect(Collectors.joining(" or "));
    return filterTable(tableServiceClient, StorageTableUtils.getDatasetTableName(), filter);
  }

  public static int getTableEntryCount(
      TableServiceClient tableServiceClient, String tableName, ListEntitiesOptions options) {
    if (options == null) {
      options = new ListEntitiesOptions();
    }
    if (tableHasEntries(tableServiceClient, tableName, options)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      return Iterables.size(entities);
    }
    return 0;
  }
}
