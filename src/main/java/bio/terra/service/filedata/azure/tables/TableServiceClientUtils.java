package bio.terra.service.filedata.azure.tables;

import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.ListTablesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableItem;

public class TableServiceClientUtils {

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
}
