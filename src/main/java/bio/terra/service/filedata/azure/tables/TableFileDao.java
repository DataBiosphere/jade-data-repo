package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.google.firestore.*;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.google.api.core.SettableApiFuture;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
  private final FireStoreUtils fireStoreUtils;
  private final ExecutorService executor;
  private final String TABLE_NAME = "files";
  private final String PARTITION_KEY = "partitionKey";
  private static final int DELETE_BATCH_SIZE = 500;

  @Autowired
  TableFileDao(
      FireStoreUtils fireStoreUtils, @Qualifier("performanceThreadpool") ExecutorService executor) {
    this.fireStoreUtils = fireStoreUtils;
    this.executor = executor;
  }

  public void createFileMetadata(TableServiceClient tableServiceClient, FireStoreFile newFile) {
    tableServiceClient.createTableIfNotExists(TABLE_NAME);
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    TableEntity entity =
        new TableEntity(PARTITION_KEY, newFile.getFileId())
            .addProperty("fileId", newFile.getFileId())
            .addProperty("mimeType", newFile.getMimeType())
            .addProperty("description", newFile.getDescription())
            .addProperty("bucketResourceId", newFile.getBucketResourceId())
            .addProperty("loadTag", newFile.getLoadTag())
            .addProperty("fileCreatedDate", newFile.getFileCreatedDate())
            .addProperty("gspath", newFile.getGspath())
            .addProperty("checksumCrc32c", newFile.getChecksumCrc32c())
            .addProperty("checksumMd5", newFile.getChecksumMd5())
            .addProperty("size", newFile.getSize());
    logger.info("creating file metadata for fileId: " + newFile.getFileId());
    tableClient.createEntity(entity);
  }

  public boolean deleteFileMetadata(TableServiceClient tableServiceClient, String fileId) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    TableEntity entity = tableClient.getEntity(PARTITION_KEY, fileId);
    if (entity == null) {
      return false;
    }
    logger.info("deleting file metadata for fileId " + fileId);
    tableClient.deleteEntity(PARTITION_KEY, fileId);
    return true;
  }

  public FireStoreFile retrieveFileMetadata(TableServiceClient tableServiceClient, String fileId) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    TableEntity entity = tableClient.getEntity(PARTITION_KEY, fileId);
    return FireStoreFile.fromTableEntity(entity);
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
      TableServiceClient tableServiceClient, List<FireStoreDirectoryEntry> directoryEntries) {
    List<FireStoreFile> files = new ArrayList<>();
    for (FireStoreDirectoryEntry f : directoryEntries) {
      FireStoreFile fsFile = retrieveFileMetadata(tableServiceClient, f.getFileId());
      if (fsFile == null) {
        throw new FileSystemCorruptException("Directory entry refers to non-existent file");
      }
      files.add(fsFile);
    }
    return files;
  }

  <V> void scanTableObjects(
      TableClient tableClient, int batchSize, ApiFutureGenerator<V, TableEntity> generator)
      throws InterruptedException {
    int batchCount = 0;
    PagedIterable<TableEntity> entities;
    do {
      ListEntitiesOptions options = new ListEntitiesOptions().setTop(batchSize);
      entities = tableClient.listEntities(options, null, null);
      batchCount++;
      if (entities.iterator().hasNext()) {
        logger.info("Visiting batch " + batchCount + " of ~" + batchSize + " documents");
      }
      List<TableEntity> entityList = ImmutableList.copyOf(entities);
      fireStoreUtils.batchOperation(entityList, generator);

    } while (entities.spliterator().getExactSizeIfKnown() > 0);
  }

  void deleteFilesFromDataset(
      TableServiceClient tableServiceClient, InterruptibleConsumer<FireStoreFile> func)
      throws InterruptedException {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    scanTableObjects(
        tableClient,
        DELETE_BATCH_SIZE,
        entity -> {
          SettableApiFuture<Boolean> future = SettableApiFuture.create();
          executor.execute(
              () -> {
                try {
                  FireStoreFile fireStoreFile = FireStoreFile.fromTableEntity(entity);
                  func.accept(fireStoreFile);
                  future.set(deleteFileMetadata(tableServiceClient, fireStoreFile.getFileId()));
                } catch (final Exception e) {
                  future.setException(e);
                }
              });
          return future;
        });
  }
}
