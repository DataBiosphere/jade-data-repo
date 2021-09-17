package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.google.firestore.FireStoreDependency;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.*;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableDependencyDao {
  private final Logger logger = LoggerFactory.getLogger(TableDependencyDao.class);
  private static final String DEPENDENCY_TABLE_NAME_SUFFIX = "dependencies";

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

    for (String refId : refIds) {
      ListEntitiesOptions options =
          new ListEntitiesOptions()
              .setFilter(String.format("fileId eq '%s' and snapshotId eq '%s'", refId, snapshotId));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      int count = 0;
      for (TableEntity entity : entities) {
        count += 1;
        if (count > 1) {
          break;
        }
      }

      switch (count) {
        case 0:
          {
            // no dependency yet. Let's make one
            FireStoreDependency fireStoreDependency =
                new FireStoreDependency()
                    .snapshotId(snapshotId.toString())
                    .fileId(refId)
                    .refCount(1L);
            TableEntity fireStoreDependencyEntity =
                FireStoreDependency.toTableEntity(snapshotId.toString(), fireStoreDependency);
            tableClient.createEntity(fireStoreDependencyEntity);
            break;
          }

        case 1:
          {
            // existing dependency; increment the reference count
            TableEntity entity = entities.iterator().next();
            FireStoreDependency fireStoreDependency = FireStoreDependency.fromTableEntity(entity);
            fireStoreDependency.refCount(fireStoreDependency.getRefCount() + 1);
            tableClient.updateEntity(
                FireStoreDependency.toTableEntity(entity.getPartitionKey(), fireStoreDependency));
            break;
          }

        default:
          throw new FileSystemCorruptException(
              "Found more than one document for a file dependency - fileId: " + refId);
      }
    }
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
