package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.google.firestore.FireStoreDependency;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableDependencyDao {
  private final Logger logger = LoggerFactory.getLogger(TableDependencyDao.class);
  private static final String DEPENDENCY_TABLE_NAME_SUFFIX = "dependencies";
  private static final int MAX_FILTER_CLAUSES = 15;

  @Autowired
  public TableDependencyDao() {}

  private String getDatasetDependencyTableName(UUID datasetId) {
    return "datarepo" + datasetId.toString().replaceAll("-", "") + DEPENDENCY_TABLE_NAME_SUFFIX;
  }

  public void storeSnapshotFileDependencies(
      TableServiceClient tableServiceClient, UUID datasetId, UUID snapshotId, List<String> refIds) {

    // We construct the snapshot file system without using transactions. We can get away with that,
    // because no one can access this snapshot during its creation.
    String dependencyTableName = getDatasetDependencyTableName(datasetId);
    tableServiceClient.createTableIfNotExists(dependencyTableName);
    TableClient tableClient = tableServiceClient.getTableClient(dependencyTableName);
    // The partition size is one less than the MAX_FILTER_CLAUSES to account for the snapshotId
    // filter
    ListUtils.partition(refIds, MAX_FILTER_CLAUSES - 1)
        .forEach(
            refIdChunk -> {
              String filter =
                  refIdChunk.stream()
                          // maybe wrap or cause in parenthesis
                          .map(
                              refId ->
                                  String.format(
                                      "%s eq '%s'", FireStoreDependency.FILE_ID_FIELD_NAME, refId))
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
                      .collect(Collectors.toList());
              // Create any entities that do not already exist
              refIdChunk.stream()
                  .filter(id -> !existing.contains(id))
                  .forEach(refId -> createDependencyEntity(tableClient, snapshotId, refId));
              // Update refCount for existing entities
              entities.forEach(entity -> updateRefCount(tableClient, entity));
            });
  }

  private void createDependencyEntity(TableClient tableClient, UUID snapshotId, String refId) {
    FireStoreDependency fireStoreDependency =
        new FireStoreDependency().snapshotId(snapshotId.toString()).fileId(refId).refCount(1L);
    TableEntity fireStoreDependencyEntity =
        FireStoreDependency.toTableEntity(fireStoreDependency);
    tableClient.createEntity(fireStoreDependencyEntity);
  }

  private void updateRefCount(TableClient tableClient, TableEntity entity) {
    FireStoreDependency fireStoreDependency = FireStoreDependency.fromTableEntity(entity);
    fireStoreDependency.refCount(fireStoreDependency.getRefCount() + 1);
    tableClient.updateEntity(
        FireStoreDependency.toTableEntity(fireStoreDependency));
  }

  public void deleteSnapshotFileDependencies(
      TableServiceClient tableServiceClient, UUID datasetId, UUID snapshotId) {
    String dependencyTableName = getDatasetDependencyTableName(datasetId);
    if (TableServiceClientUtils.tableHasEntries(tableServiceClient, dependencyTableName)) {
      TableClient tableClient = tableServiceClient.getTableClient(dependencyTableName);
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("snapshotId eq '%s'", snapshotId));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      var batchEntities =
          entities.stream()
              .map(entity -> new TableTransactionAction(TableTransactionActionType.DELETE, entity))
              .collect(Collectors.toList());
      logger.info(
          "Deleting snapshot {} file dependencies from {}", snapshotId, dependencyTableName);
      tableClient.submitTransaction(batchEntities);
    } else {
      logger.warn("No snapshot file dependencies found to be deleted from dataset");
    }
  }
}
