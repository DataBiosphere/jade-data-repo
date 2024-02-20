package bio.terra.service.filedata.azure.tables;

import bio.terra.common.FutureUtils;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.firestore.FireStoreDependency;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class TableDependencyDao {
  private final Logger logger = LoggerFactory.getLogger(TableDependencyDao.class);

  private static final int MAX_FILTER_CLAUSES = 15;

  private final AsyncTaskExecutor azureTableThreadpool;

  @Autowired
  public TableDependencyDao(AsyncTaskExecutor azureTableThreadpool) {
    this.azureTableThreadpool = azureTableThreadpool;
  }

  public void storeSnapshotFileDependencies(
      TableServiceClient tableServiceClient, UUID datasetId, UUID snapshotId, List<String> refIds) {

    // We construct the snapshot file system without using transactions. We can get away with that,
    // because no one can access this snapshot during its creation.
    String dependencyTableName = StorageTableName.DEPENDENCIES.toTableName(datasetId);
    tableServiceClient.createTableIfNotExists(dependencyTableName);
    TableClient tableClient = tableServiceClient.getTableClient(dependencyTableName);
    // The partition size is one less than the MAX_FILTER_CLAUSES to account for the snapshotId
    // filter
    List<Future<Void>> futures = new ArrayList<>();
    for (List<String> refIdChunk : ListUtils.partition(refIds, MAX_FILTER_CLAUSES - 1)) {
      futures.add(
          azureTableThreadpool.submit(
              () -> {
                String filter =
                    refIdChunk.stream()
                            // maybe wrap or cause in parenthesis
                            .map(
                                refId ->
                                    String.format(
                                        "%s eq '%s'",
                                        FireStoreDependency.FILE_ID_FIELD_NAME, refId))
                            .collect(Collectors.joining(" or "))
                        + String.format(
                            " and %s eq '%s'",
                            FireStoreDependency.SNAPSHOT_ID_FIELD_NAME, snapshotId);
                List<TableEntity> entities =
                    TableServiceClientUtils.filterTable(
                        tableServiceClient, dependencyTableName, filter);
                List<String> existing =
                    entities.stream()
                        .map(e -> e.getProperty(FireStoreDependency.FILE_ID_FIELD_NAME).toString())
                        .toList();
                // Create any entities that do not already exist
                List<TableTransactionAction> batchEntities =
                    refIdChunk.stream()
                        .distinct()
                        .filter(id -> !existing.contains(id))
                        .map(
                            refId -> {
                              FireStoreDependency fireStoreDependency =
                                  new FireStoreDependency()
                                      .snapshotId(snapshotId.toString())
                                      .fileId(refId)
                                      .refCount(1L);
                              TableEntity fireStoreDependencyEntity =
                                  FireStoreDependency.toTableEntity(fireStoreDependency);
                              return new TableTransactionAction(
                                  TableTransactionActionType.UPSERT_REPLACE,
                                  fireStoreDependencyEntity);
                            })
                        .toList();
                if (!batchEntities.isEmpty()) {
                  // This can happen if retrying a failed transaction.  This would cause an
                  // IllegalArgumentException with message "A transaction must contain at least one
                  // operation"
                  tableClient.submitTransaction(batchEntities);
                }
                return null;
              }));
    }
    FutureUtils.waitFor(futures);
  }

  public void deleteSnapshotFileDependencies(
      TableServiceClient tableServiceClient, UUID datasetId, UUID snapshotId) {
    String dependencyTableName = StorageTableName.DEPENDENCIES.toTableName(datasetId);

    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, dependencyTableName)) {
      TableClient tableClient = tableServiceClient.getTableClient(dependencyTableName);
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("snapshotId eq '%s'", snapshotId));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      logger.info(
          "Deleting snapshot {} file dependencies from {}", snapshotId, dependencyTableName);
      entities.stream().forEach(tableClient::deleteEntity);
    } else {
      logger.warn("No snapshot file dependencies found to be deleted from dataset");
    }
  }

  public List<String> getDatasetSnapshotFileIds(
      TableServiceClient tableServiceClient, Dataset dataset, String snapshotId) {
    String dependencyTableName = StorageTableName.DEPENDENCIES.toTableName(dataset.getId());
    TableClient tableClient = tableServiceClient.getTableClient(dependencyTableName);
    ListEntitiesOptions options =
        new ListEntitiesOptions().setFilter(String.format("snapshotId eq '%s'", snapshotId));
    return tableClient.listEntities(options, null, null).stream()
        .map(FireStoreDependency::fromTableEntity)
        .map(FireStoreDependency::getFileId)
        .toList();
  }

  public boolean datasetHasSnapshotReference(
      TableServiceClient tableServiceClient, UUID datasetId) {
    String dependencyTableName = StorageTableName.DEPENDENCIES.toTableName(datasetId);
    return TableServiceClientUtils.tableHasEntries(tableServiceClient, dependencyTableName);
  }
}
