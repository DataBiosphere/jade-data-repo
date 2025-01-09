package bio.terra.service.filedata.azure.tables;

import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.ListTablesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableServiceClientUtils {
  private static final Logger logger = LoggerFactory.getLogger(TableServiceClientUtils.class);

  public static boolean tableHasEntries(
      TableServiceClient tableServiceClient, String tableName, ListEntitiesOptions options) {
    if (options == null) {
      options = new ListEntitiesOptions();
    }
    if (tableExists(tableServiceClient, tableName)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      options.setTop(1);
      try {
        PagedIterable<TableEntity> tableEntities = tableClient.listEntities(options, null, null);
        if (tableEntities.iterator().hasNext()) {
          return true;
        }
      } catch (TableServiceException ex) {
        logger.error("Error listing entities in table {}", tableName, ex);
        return false;
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
    try {
      var result = tableServiceClient.listTables(options, null, null);
      logger.info("Table {} exists", tableName);
      return result.iterator().hasNext();
    } catch (TableServiceException ex) {
      logger.error("Error listing tables with table name filter " + tableName, ex);
      return false;
    }
  }

  public static List<TableEntity> filterTable(
      TableServiceClient tableServiceClient, String tableName, String filter) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    ListEntitiesOptions options = new ListEntitiesOptions().setFilter(filter);
    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
      try {
        var result = tableClient.listEntities(options, null, null).stream().toList();
        logger.info("Found {} entities in table {}", result.size(), tableName);
        return result;
      } catch (TableServiceException ex) {
        throw new FileSystemExecutionException(
            String.format("Error listing entities from table %s with filter %s", tableName, filter),
            ex);
      }
    }
    logger.info("No entities found in table {} with filter {}", tableName, filter);
    return Collections.emptyList();
  }

  public static List<TableEntity> batchRetrieveFiles(
      TableServiceClient tableServiceClient, UUID datasetId, List<String> fileIdArray) {
    String filter =
        fileIdArray.stream()
            // maybe wrap or cause in parentheses
            .map(refId -> String.format("fileId eq '%s'", refId))
            .collect(Collectors.joining(" or "));
    return filterTable(tableServiceClient, StorageTableName.DATASET.toTableName(datasetId), filter);
  }

  /**
   * Allows us to check that there is a unique entry for a given storage table and parameters passed
   * through 'ListEntitiesOptions'
   *
   * @param tableServiceClient - client to use to query storage tables
   * @param tableName - storage table to check for entry
   * @param options - filter entities returned in list
   * @return
   */
  public static boolean tableHasSingleEntry(
      TableServiceClient tableServiceClient, String tableName, ListEntitiesOptions options) {
    if (options == null) {
      options = new ListEntitiesOptions();
    }
    options.setTop(2);
    if (tableHasEntries(tableServiceClient, tableName, options)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      try {
        PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
        Iterator<TableEntity> iter = entities.iterator();
        // Since hasHasEntries = true, we expect there to be at least one entry
        iter.next();
        // Test for exactly one entry - the next hasNext() should return false
        return !iter.hasNext();
      } catch (TableServiceException ex) {
        logger.error(
            String.format(
                "Error listing entities from table %s with filter %s",
                tableName, options.getFilter()),
            ex);
      }
    }
    return false;
  }
}
