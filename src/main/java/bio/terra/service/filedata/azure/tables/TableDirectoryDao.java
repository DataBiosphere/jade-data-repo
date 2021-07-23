package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Paths and document names Azure uses forward slash (/) for its path separator. We also use forward
 * slash in our file system paths. To get uniqueness of files, we want to name files with their full
 * path. Otherwise, two threads could create the same file as two different documents. That would
 * not do at all.
 *
 * <p>We solve this problem by using Azure document names that replace the forward slash in our
 * paths with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C) That replacement is
 * <strong>only</strong> used for the Azure document names. All the other paths we process use the
 * forward slash separator.
 *
 * <p>We need a root directory to hold the other directories. Since we are doing Azure lookup by
 * document name, the root directory needs a name. We call it "_dr_"; it could be anything, but it
 * helps when viewing Azure in the console that it has an obvious name.
 *
 * <p>We don't store the root directory in the paths stored in file and directory entries. It is
 * only used for the Azure lookup. When we refer to paths in the code we talk about: - lookup path -
 * the path used for the Azure lookup. When building this path (and only this path) we prepended it
 * with "_dr_" as the name for the root directory. - directory path - the directory path to the
 * directory containing entry - not including the entry name - full path - the full path to the
 * entry including the entry name.
 *
 * <p>Within the document we store the directory path to the entry and the entry name. That lets us
 * use the indexes to find the entries in a directory.
 *
 * <p>It is an invariant that there are no empty directories. When a directory becomes empty on a
 * delete, it is deleted. When a directory is needed, we create it. That is all done within
 * transactions so there is never a time where the externally visible state violates that invariant.
 */
@Component
public class TableDirectoryDao {
  private final Logger logger = LoggerFactory.getLogger(FireStoreDirectoryDao.class);

  private static final String ROOT_DIR_NAME = "/_dr_";
  private static final String TABLE_NAME = "dataset";
  private static final String PARTITION_KEY = "partitionKey";

  private final AzureTableUtils azureTableUtils;
  private final FireStoreUtils fireStoreUtils;

  @Autowired
  public TableDirectoryDao(AzureTableUtils azureTableUtils, FireStoreUtils fireStoreUtils) {
    this.azureTableUtils = azureTableUtils;
    this.fireStoreUtils = fireStoreUtils;
  }

  // Note that this does not test for duplicates. If invoked on an existing path it will overwrite
  // the entry. Existence checking is handled at upper layers.
  public void createDirectoryEntry(
      TableServiceClient tableServiceClient, FireStoreDirectoryEntry createEntry) {

    tableServiceClient.createTableIfNotExists(TABLE_NAME);
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);

    List<TableTransactionAction> createList = new ArrayList<>();

    // Walk up the lookup directory path, finding missing directories we get to an
    // existing one
    // We will create the ROOT_DIR_NAME directory here if it does not exist.
    String lookupDirPath = makeLookupPath(createEntry.getPath());
    for (String testPath = lookupDirPath;
        !testPath.isEmpty();
        testPath = fireStoreUtils.getDirectoryPath(testPath)) {

      // !!! In this case we are using a lookup path
      if (lookupByFilePath(tableServiceClient, testPath) != null) {
        break;
      }

      FireStoreDirectoryEntry dirToCreate = makeDirectoryEntry(testPath);
      TableEntity entity = FireStoreDirectoryEntry.toTableEntity(PARTITION_KEY, dirToCreate);
      TableTransactionAction t =
          new TableTransactionAction(TableTransactionActionType.CREATE, entity);
      createList.add(t);
    }

    TableEntity createEntryEntity =
        FireStoreDirectoryEntry.toTableEntity(PARTITION_KEY, createEntry);
    TableTransactionAction createEntryTransaction =
        new TableTransactionAction(TableTransactionActionType.CREATE, createEntryEntity);
    createList.add(createEntryTransaction);

    logger.info("Creating file directory for table {}", TABLE_NAME);
    tableClient.submitTransaction(createList);
  }

  // true - directory entry existed and was deleted; false - directory entry did not exist
  public boolean deleteDirectoryEntry(TableServiceClient tableServiceClient, String fileId) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);

    // Look up the directory entry by id. If it doesn't exist, we're done
    TableEntity leafEntity = lookupByFileId(tableServiceClient, fileId);
    if (leafEntity == null) {
      return false;
    }

    List<TableTransactionAction> deleteList = new ArrayList<>();
    TableTransactionAction t =
        new TableTransactionAction(TableTransactionActionType.DELETE, leafEntity);
    deleteList.add(t);

    FireStoreDirectoryEntry leafEntry = FireStoreDirectoryEntry.fromTableEntity(leafEntity);
    String lookupPath = makeLookupPath(leafEntry.getPath());
    while (!lookupPath.isEmpty()) {
      // Count the number of entries with this path as their directory path
      // A value of 1 means that the directory will be empty after its child is
      // deleted, so we should delete it also.
      ListEntitiesOptions options =
          new ListEntitiesOptions()
              .setFilter(String.format("path eq '%s'", makePathFromLookupPath(lookupPath)));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      int size = List.of(entities).size();
      if (size > 1) {
        break;
      }
      TableEntity entity =
          lookupByFilePath(tableServiceClient, encodePathAsFirestoreDocumentName(lookupPath));
      deleteList.add(new TableTransactionAction(TableTransactionActionType.DELETE, entity));
      lookupPath = fireStoreUtils.getDirectoryPath(lookupPath);
    }
    tableClient.submitTransaction(deleteList);
    return true;
  }

  // Each dataset/snapshot has its own set of tables, so we delete the entire directory entry table
  public void deleteDirectoryEntriesFromCollection(TableServiceClient tableServiceClient) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    tableClient.deleteTable();
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveById(
      TableServiceClient tableServiceClient, String fileId) {
    TableEntity entity = lookupByFileId(tableServiceClient, fileId);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveByPath(
      TableServiceClient tableServiceClient, String fullPath) {
    String lookupPath = makeLookupPath(fullPath);
    TableEntity entity = lookupByFilePath(tableServiceClient, lookupPath);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  public List<String> validateRefIds(
      TableServiceClient tableServiceClient, List<String> refIdArray) {
    logger.info("validateRefIds for {} file ids", refIdArray.size());
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    List<String> missingIds = new ArrayList<>();
    for (String s : refIdArray) {
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("fileId eq '%s'", s));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      if (!entities.iterator().hasNext()) {
        missingIds.add(s);
      }
    }
    return missingIds;
  }

  List<FireStoreDirectoryEntry> enumerateDirectory(
      TableServiceClient tableServiceClient, String dirPath) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    ListEntitiesOptions options =
        new ListEntitiesOptions().setFilter(String.format("path eq '%s'", dirPath));
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    return entities.stream()
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .collect(Collectors.toList());
  }

  // As mentioned at the top of the module, we can't use forward slash in a FireStore document
  // name, so we do this encoding.
  private static final char DOCNAME_SEPARATOR = '\u001c';

  private String encodePathAsFirestoreDocumentName(String path) {
    return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
  }

  private TableEntity lookupByFilePath(TableServiceClient tableServiceClient, String lookupPath) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    String rowKey = encodePathAsFirestoreDocumentName(lookupPath);
    try {
      return tableClient.getEntity(PARTITION_KEY, rowKey);
    } catch (TableServiceException ex) {
      return null;
    }
  }

  // Returns null if not found
  private TableEntity lookupByFileId(TableServiceClient tableServiceClient, String fileId) {
    try {
      TableClient client = tableServiceClient.getTableClient(TABLE_NAME);
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("fileId eq '%s'", fileId));
      PagedIterable<TableEntity> entities = client.listEntities(options, null, null);
      if (!entities.iterator().hasNext()) {
        return null;
      }
      int count = 0;
      for (TableEntity entity : entities) {
        count += 1;
        if (count > 1) {
          logger.warn("Found more than one entry for file {}", fileId);
          throw new FileSystemAbortTransactionException("lookupByFileId found too many entries");
        }
      }
      return entities.iterator().next();

    } catch (TableServiceException ex) {
      throw new FileSystemExecutionException("lookupByFileId operation failed");
    }
  }

  private FireStoreDirectoryEntry makeDirectoryEntry(String lookupDirPath) {
    // We have some special cases to deal with at the top of the directory tree.
    String fullPath = makePathFromLookupPath(lookupDirPath);
    String dirPath = fireStoreUtils.getDirectoryPath(fullPath);
    String objName = fireStoreUtils.getName(fullPath);
    if (StringUtils.isEmpty(fullPath)) {
      // This is the root directory - it doesn't have a path or a name
      dirPath = StringUtils.EMPTY;
      objName = StringUtils.EMPTY;
    } else if (StringUtils.isEmpty(dirPath)) {
      // This is an entry in the root directory - it needs to have the root path.
      dirPath = "/";
    }

    return new FireStoreDirectoryEntry()
        .fileId(UUID.randomUUID().toString())
        .isFileRef(false)
        .path(dirPath)
        .name(objName)
        .fileCreatedDate(Instant.now().toString());
  }

  // Do some tidying of the full path: slash on front - no slash trailing
  // and prepend the root directory name
  private String makeLookupPath(String fullPath) {
    String temp = StringUtils.prependIfMissing(fullPath, "/");
    temp = StringUtils.removeEnd(temp, "/");
    return ROOT_DIR_NAME + temp;
  }

  private String makePathFromLookupPath(String lookupPath) {
    return StringUtils.removeStart(lookupPath, ROOT_DIR_NAME);
  }
}
