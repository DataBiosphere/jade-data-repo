package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.*;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.UnavailableException;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.grpc.StatusRuntimeException;
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
  private final ExecutorService executor;
  private static final String TABLE_NAME = "files";
  private static final String PARTITION_KEY = "partitionKey";
  private static final int SLEEP_BASE_SECONDS = 1;
  private static final int AZURE_STORAGE_RETRIES = 1;


  @Autowired
  TableFileDao(@Qualifier("performanceThreadpool") ExecutorService executor) {
    this.executor = executor;
  }

  public void createFileMetadata(TableServiceClient tableServiceClient, FireStoreFile newFile) {
    tableServiceClient.createTableIfNotExists(TABLE_NAME);
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    TableEntity entity = FireStoreFile.toTableEntity(PARTITION_KEY, newFile);
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
    return directoryEntries.stream()
        .map(
            f ->
                Optional.ofNullable(retrieveFileMetadata(tableServiceClient, f.getFileId()))
                    .orElseThrow(
                        () ->
                            new FileSystemCorruptException(
                                "Directory entry refers to non-existent file")))
        .collect(Collectors.toList());
  }

  <V> void scanTableObjects(
      TableClient tableClient, ApiFutureGenerator<V, TableEntity> generator) {
    ListEntitiesOptions options = new ListEntitiesOptions();
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    entities.stream().forEach(entity -> {
      try {
        retryOperation(entity, generator);
      } catch (InterruptedException e) {
        throw new FileSystemExecutionException("operation failed", e);
      }
    });
  }

  <T, V> void retryOperation(V input, ApiFutureGenerator<T, V> generator) throws InterruptedException {
      int retry = 0;
      while(true) {
        try {
          generator.accept(input).get();
          break;
        } catch (DeadlineExceededException
                | UnavailableException
                | AbortedException
                | InternalException
                | StatusRuntimeException
                | ExecutionException ex) {
          if (FireStoreUtils.shouldRetry(ex, false)) {
            logger.warn(
                    "Retry-able error in Azure storage table future get - input: "
                            + input.toString()
                            + " message: "
                            + ex.getMessage());
          } else throw new FileSystemExecutionException("operation failed", ex);
        }

        final long retryWait = (long) (SLEEP_BASE_SECONDS * Math.pow(2.5, retry));
        retry++;
        if (retry > AZURE_STORAGE_RETRIES) {
          throw new FileSystemExecutionException(
                  "operation failed. " + AZURE_STORAGE_RETRIES + " tries with no progress.");
        } else {
          logger.info(
                  "[Operation] will attempt retry #{} after {} millisecond pause.",
                  retry,
                  retryWait);
        }

        // Exponential backoff
        TimeUnit.SECONDS.sleep(retryWait);
      }
  };

  void deleteFilesFromDataset(
      TableServiceClient tableServiceClient, InterruptibleConsumer<FireStoreFile> func)
      throws InterruptedException {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    scanTableObjects(
        tableClient,
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
