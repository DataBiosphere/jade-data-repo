package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * TableFileDao provides CRUD operations on the "files" storage table in Azure. Objects in the
 * file table are referred to by the dataset (owner of the files) and by any snapshots that
 * reference the files. File naming is handled by the directory DAO. This DAO just handles the basic
 * operations for managing the collection. It does not have logic for protecting against deleting
 * files that have dependencies or creating files with duplicate paths.
 *
 */
@Component
class TableFileDao {
    private final Logger logger = LoggerFactory.getLogger(TableFileDao.class);
    private final String TABLE_NAME = "files";
    private final String PARTITION_KEY = "partitionKey";

    @Autowired
    TableFileDao() {
    }

    void createFileMetadata(TableServiceClient tableServiceClient, FireStoreFile newFile) {
        TableClient tableClient = tableServiceClient.createTableIfNotExists(TABLE_NAME);
        TableEntity entity =
                new TableEntity(PARTITION_KEY, newFile.getFileId())
                        .addProperty("fileId", newFile.getFileId())
                        .addProperty("mimeType", newFile.getMimeType())
                        .addProperty("description", newFile.getDescription())
                        .addProperty("bucketResourceId", newFile.getBucketResourceId())
                        .addProperty("loadTag", newFile.getLoadTag())
                        .addProperty("fileCreatedDate", newFile.getFileCreatedDate())
                        .addProperty("gsPath", newFile.getGspath())
                        .addProperty("checksumCrc32c", newFile.getChecksumCrc32c())
                        .addProperty("checksumMd5", newFile.getChecksumMd5())
                        .addProperty("size", newFile.getSize());
        logger.info("creating file metadata for fileId: " + newFile.getFileId());
        tableClient.createEntity(entity);
    }

    void deleteFileMetadata(TableServiceClient tableServiceClient, String fileId) {
        TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
        logger.info("deleting file metadata for fileId " + fileId);
        tableClient.deleteEntity(PARTITION_KEY, fileId);
    }

    FireStoreFile retrieveFileMetadata(TableServiceClient tableServiceClient, String fileId) {
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

//    void deleteFilesFromDataset(Firestore firestore, String datasetId, InterruptibleConsumer<FireStoreFile> func)
//      throws InterruptedException {
//            // Query "files" table for DELETE_BATCH_SIZE (500) entries
//            // Delete the actual file from Azure
//            // Delete the entry
//    }

}
