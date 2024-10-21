package bio.terra.service.filedata.azure.tables;

import static bio.terra.service.common.azure.StorageTableName.FILES_TABLE;

import bio.terra.common.FutureUtils;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.ApiFutureGenerator;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.firestore.InterruptibleConsumer;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.google.api.core.SettableApiFuture;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * TableFileDao provides CRUD operations on the "files" storage table in Azure. Objects in the file
 * table are referred to by the dataset (owner of the files) and by any snapshots that reference the
 * files. File naming is handled by the directory DAO. This DAO just handles the basic operations
 * for managing the collection. It does not have logic for protecting against deleting files that
 * have dependencies or creating files with duplicate paths.
 */
@Component
public class TableFileDao {
  private final Logger logger = LoggerFactory.getLogger(TableFileDao.class);
  private final AsyncTaskExecutor azureTableThreadpool;
  private static final String PARTITION_KEY = "partitionKey";

  TableFileDao(
      @Qualifier(AzureResourceConfiguration.TABLE_THREADPOOL_NAME)
          AsyncTaskExecutor azureTableThreadpool) {
    this.azureTableThreadpool = azureTableThreadpool;
  }

  public void createFileMetadata(
      TableServiceClient tableServiceClient, String collectionId, FireStoreFile newFile) {
    tableServiceClient.createTableIfNotExists(
        FILES_TABLE.toTableName(UUID.fromString(collectionId)));
    TableClient tableClient =
        tableServiceClient.getTableClient(FILES_TABLE.toTableName(UUID.fromString(collectionId)));
    TableEntity entity = FireStoreFile.toTableEntity(PARTITION_KEY, newFile);
    logger.info("creating file metadata for fileId {}", newFile.getFileId());
    tableClient.createEntity(entity);
  }

  public boolean deleteFileMetadata(
      TableServiceClient tableServiceClient, String collectionId, String fileId) {
    TableClient tableClient =
        tableServiceClient.getTableClient(FILES_TABLE.toTableName(UUID.fromString(collectionId)));
    try {
      logger.info("deleting file metadata for fileId {}", fileId);
      TableEntity entity = tableClient.getEntity(PARTITION_KEY, fileId);
      tableClient.deleteEntity(entity);
      return true;
    } catch (TableServiceException ex) {
      logger.warn(
          "Error deleting file metadata for fileId {}, message: {}", fileId, ex.getMessage());
      return false;
    }
  }

  public FireStoreFile retrieveFileMetadata(
      TableServiceClient tableServiceClient, String collectionId, String fileId) {
    try {
      TableClient tableClient =
          tableServiceClient.getTableClient(FILES_TABLE.toTableName(UUID.fromString(collectionId)));
      TableEntity entity = tableClient.getEntity(PARTITION_KEY, fileId);
      return FireStoreFile.fromTableEntity(entity);
    } catch (TableServiceException ex) {
      // enable listFiles to work that have file ingests before the DC-1259 fix
      // This is a temporary fix to ignore directory entries that do not have matching file entries
      logger.warn("Error retrieving file metadata for fileId: {}", fileId);
      return null;
    }
  }

  /**
   * Retrieve metadata from a list of directory entries.
   *
   * @param tableServiceClient An Azure table service client
   * @param directoryEntries List of objects to retrieve metadata for
   * @return A list of metadata object for the specified files. Note: the order of the list matches
   *     with the order of the input list objects
   */
  List<FireStoreFile> batchRetrieveFileMetadata(
      TableServiceClient tableServiceClient,
      String collectionId,
      List<FireStoreDirectoryEntry> directoryEntries) {
    return FutureUtils.waitFor(
        directoryEntries.stream()
            .map(
                f ->
                    azureTableThreadpool.submit(
                        () ->
                            retrieveFileMetadata(tableServiceClient, collectionId, f.getFileId())))
            .toList());
  }

  <V> void scanTableObjects(TableClient tableClient, ApiFutureGenerator<V, TableEntity> generator) {
    ListEntitiesOptions options = new ListEntitiesOptions();
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    if (!entities.iterator().hasNext()) {
      logger.warn("No files found in Storage Table Directory to be removed");
      return;
    }
    entities.stream()
        .forEach(
            entity -> {
              try {
                generator.accept(entity).get();
              } catch (InterruptedException | ExecutionException e) {
                throw new FileSystemExecutionException("operation failed", e);
              }
            });
  }

  void deleteFilesFromDataset(
      TableServiceClient tableServiceClient,
      UUID datasetId,
      InterruptibleConsumer<FireStoreFile> func) {
    TableClient tableClient = tableServiceClient.getTableClient(FILES_TABLE.toTableName(datasetId));

    if (TableServiceClientUtils.tableHasEntries(
        tableServiceClient, FILES_TABLE.toTableName(datasetId))) {
      scanTableObjects(
          tableClient,
          entity -> {
            SettableApiFuture<Boolean> future = SettableApiFuture.create();
            azureTableThreadpool.execute(
                () -> {
                  try {
                    FireStoreFile fireStoreFile = FireStoreFile.fromTableEntity(entity);
                    func.accept(fireStoreFile);
                    future.set(
                        deleteFileMetadata(
                            tableServiceClient, datasetId.toString(), fireStoreFile.getFileId()));
                  } catch (final Exception e) {
                    future.setException(e);
                  }
                });
            return future;
          });

    } else {
      logger.warn("No files found to be deleted from dataset.");
    }

    // Delete the table
    tableClient.deleteTable();
  }
}
