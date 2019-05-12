package bio.terra.filesystem;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObject;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.Transaction;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Paths and document names
 * FireStore uses forward slash (/) for its path separator. We also use forward slash in our
 * file system paths. To get uniqueness of objects, we want to name objects with their full
 * path. Otherwise, two threads could create the same file as two different documents. That
 * would not do at all.
 *
 * We solve this problem by using document names that replace the forward slash in our paths
 * with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C)
 *
 * Within the document we store the directory path to the object and the object name. That
 * lets us use the indexes to find the objects in a directory using the index.
 *
 * We use FireStore transactions. The required transaction pattern is always read-modify-write.
 * The transactions are expressed as functions that are retried if another transaction touches
 * the object between our transaction's read and write.
 */


@Repository
public class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreFileDao");

    private static final char DOCNAME_SEPARATOR = '\u001c';

    private Firestore firestore;

    @Autowired
    public FireStoreFileDao(Firestore firestore) {
        this.firestore = firestore;
    }

    public UUID createFileStart(FSObject fileToCreate) {
        if (fileToCreate.getObjectType() != FSObject.FSObjectType.INGESTING_FILE) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }

        // Walk up the directory path, creating directories as necessary until we get to an existing one
        for (String testPath = getDirectoryPath(fileToCreate.getPath());
             testPath != null;
             testPath = getDirectoryPath(testPath)) {

            FireStoreObject dirToCreate = makeDirectoryObject(fileToCreate.getStudyId(), testPath);
            UUID objectId = createObject(dirToCreate);
            if (objectId == null) {
                break;
            }
        }

        FireStoreObject fireStoreFile = makeFileObjectFromFSObject(fileToCreate);
        return createObject(fireStoreFile);
    }

    public boolean createFileStartUndo(UUID studyId, UUID objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            if (!StringUtils.equals(flightId, currentObject.getFlightId())) {
                throw new InvalidFileSystemObjectTypeException(
                    "Invalid attempt to delete a file being ingested by a different flight");
            }
            if (!StringUtils.equals(FSObject.FSObjectType.INGESTING_FILE.toLetter(),
                currentObject.getObjectTypeLetter())) {
                throw new FileSystemCorruptException("Attempt to deleteFileForCreateUndo with bad file object type");
            }

            xn.delete(docSnap.getReference());
            // TODO: delete empty parent directories
            return true;
        });

        return transactionGet("create start undo", transaction);
    }

    public UUID createFileComplete(FSFileInfo fsFileInfo) {
        ApiFuture<UUID> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(fsFileInfo.getStudyId(), fsFileInfo.getObjectId(), xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject
                .objectTypeLetter(FSObject.FSObjectType.FILE.toLetter())
                .gspath(fsFileInfo.getGspath())
                .checksumMd5(fsFileInfo.getChecksumMd5())
                .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
                .size(fsFileInfo.getSize())
                .fileCreatedDate(fsFileInfo.getCreatedDate());

            xn.set(docSnap.getReference(), currentObject);
            return UUID.fromString(currentObject.getObjectId());
        });

        return transactionGet("create complete", transaction);
    }

    public void createFileCompleteUndo(UUID studyId, UUID objectId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject.objectTypeLetter(FSObject.FSObjectType.INGESTING_FILE.toLetter())
            xn.set(docSnap.getReference(), currentObject);
            return UUID.fromString(currentObject.getObjectId());
        });

        transactionGet("create complete undo", transaction);
    }

    public boolean deleteFileStart(UUID studyId, UUID objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case FILE:
                    break;
                case DIRECTORY:
                    throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                    }
                    break;
                case INGESTING_FILE:
                    throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            if (hasDatasetReferences(objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one dataset and cannot be deleted");
            }

            currentObject
                .objectTypeLetter(FSObject.FSObjectType.DELETING_FILE.toLetter())
                .flightId(flightId);
            xn.set(docSnap.getReference(), currentObject);
            return true;
        });

        return transactionGet("delete file start", transaction);
    }

    public boolean deleteFileComplete(UUID studyId, UUID objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case FILE:
                    throw new CorruptMetadataException("File is not marked for deletion");
                case DIRECTORY:
                    throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                    }
                    break;
                case INGESTING_FILE:
                    throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            if (hasDatasetReferences(objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one dataset and cannot be deleted");
            }

            xn.delete(docSnap.getReference());
            // TODO: delete empty directories
            return true;
        });

        return transactionGet("delete file start", transaction);
    }

    public void deleteFileStartUndo(UUID studyId, UUID objectId, String flightId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        return;
                    }
                    break;

                case FILE:
                case DIRECTORY:
                case INGESTING_FILE:
                    return;
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            currentObject.objectTypeLetter(FSObject.FSObjectType.FILE.toLetter())
            xn.set(docSnap.getReference(), currentObject);
            return UUID.fromString(currentObject.getObjectId());
        });

        transactionGet("delete start undo", transaction);
    }


// TODO: ------ PRIVATES ------

    // Returns null if object exists; returns new UUID when object is created
    private UUID createObject(FireStoreObject fireStoreObject) {
        DocumentReference docRef = getDocRef(fireStoreObject);

        ApiFuture<UUID> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot snapshot = xn.get(docRef).get();
            if (snapshot.exists()) {
                return null;
            }
            UUID objectId = UUID.randomUUID();
            fireStoreObject.objectId(objectId.toString());
            xn.set(docRef, fireStoreObject);
            return objectId;
        });

        return transactionGet("create directory", transaction);
    }

    private DocumentReference getDocRef(FireStoreObject fireStoreObject) {
        return firestore.collection(fireStoreObject.getStudyId()).document(fireStoreObject.getDocumentName());
    }

        private String getObjectName(String path) {
            String[] pathParts = StringUtils.split(path, '/');
            return pathParts[pathParts.length - 1];
        }

        private String getDirectoryPath(String path) {
            String[] pathParts = StringUtils.split(path, '/');
            if (pathParts.length == 1) {
                // We are at the root; no containing directory
                return null;
            }
            int endIndex = pathParts.length - 1;
            return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
        }

    // Returns null if not found
    private QueryDocumentSnapshot lookupByObjectId(String studyId, String objectId, Transaction xn) {
        try {
            CollectionReference studyCollection = firestore.collection(studyId);
            Query query = studyCollection.whereEqualTo("objectId", objectId);
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            if (documents.size() == 0) {
                return null;
            }
            if (documents.size() != 1) {
                throw new FileSystemCorruptException("lookup by object id found too many objects");
            }

            return documents.get(0);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("lookup object id - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookup object id - execution exception", ex);
        }
    }

    private FireStoreObject makeDirectoryObject(UUID studyId, String dirPath) {
            return new FireStoreObject()
                .studyId(studyId.toString())
                .objectTypeLetter(FSObject.FSObjectType.DIRECTORY.toLetter())
                .path(getDirectoryPath(dirPath))
                .name(getObjectName(dirPath))
                .size(0L);
        }

    private FireStoreObject makeFileObjectFromFSObject(FSObject fsObject) {
        String objectId = (fsObject.getObjectId() == null) ? null : fsObject.getObjectId().toString();
        return new FireStoreObject()
            .objectId(objectId)
            .studyId(fsObject.getStudyId().toString())
            .objectTypeLetter(fsObject.getObjectType().toLetter())
            .fileCreatedDate(fsObject.getCreatedDate().toString())
            .path(getDirectoryPath(fsObject.getPath()))
            .name(getObjectName(fsObject.getPath()))
            .gspath(fsObject.getGspath())
            .checksumCrc32c(fsObject.getChecksumCrc32c())
            .checksumMd5(fsObject.getChecksumMd5())
            .size(fsObject.getSize())
            .mimeType(fsObject.getMimeType())
            .description(fsObject.getDescription())
            .flightId(fsObject.getFlightId());
    }

    private <T> T transactionGet(String op, ApiFuture<T> transaction) {
        try {
            return transaction.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException(op + " - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException(op + " - execution exception", ex);
        }

    }


// TODO: -----------------------------------------------------------------------------------------------


    // The worker assumes all object type checks have been done by the caller. If verifies that the file is
    // not used in a dataset and performs the delete of the object and any empty parents. Some paths to this
    // worker may have already checked the dataset references. For instance the file delete flight checks for
    // dependencies in its start step and this will re-check in its complete step.
    private boolean deleteFileWorker(FSObject fsObject) {
        if (hasDatasetReferences(fsObject.getObjectId())) {
            throw new FileSystemObjectDependencyException(
                "File is used by at least one dataset and cannot be deleted");
        }
        if (deleteObject(fsObject.getObjectId())) {
            deleteEmptyParents(fsObject.getStudyId(), fsObject.getPath());
        }
        return true;
    }

    void deleteEmptyParents(UUID studyId, String path) {
        String parentPath = getContainingDirectoryPath(path);
        if (parentPath == null) {
            // We are at the '/'. Nowhere else to go.
            return;
        }

        String matcher = parentPath + '%';
        String sql = "SELECT COUNT(*) AS match_count FROM fs_object" +
            " WHERE study_id = :study_id AND path LIKE :matcher";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("study_id", studyId)
            .addValue("matcher", matcher);
        Long matchCount  = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
            rs.getLong("match_count"));
        if (matchCount == null) {
            throw new CorruptMetadataException("Match count query returned no data");
        }
        if (matchCount > 1) {
            // There is an object other than directory with the path; that means the directory is not empty
            return;
        }

        FSObject fsObject = retrieveByPathNoThrow(studyId, parentPath);
        if (hasDatasetReferences(fsObject.getObjectId())) {
            return;
        }

        if (deleteObject(fsObject.getObjectId())) {
            deleteEmptyParents(studyId, parentPath);
        }
    }

    private boolean deleteObject(UUID objectId) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM fs_object WHERE object_id = :object_id",
            new MapSqlParameterSource().addValue("object_id", objectId));
        return rowsAffected > 0;
    }

    /**
     * @param objectId
     * @return true if the object is referenced in any dataset; false otherwise
     */
    boolean hasDatasetReferences(UUID objectId) {
        String sql = "SELECT COUNT(*) AS dataset_count FROM fs_dataset WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("object_id", objectId);
        Long datasetCount = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
            rs.getLong("dataset_count"));
        if (datasetCount == null) {
            throw new CorruptMetadataException("Dataset count query returned no data");
        }

        return (datasetCount > 0);
    }

    public FSObject retrieve(UUID objectId) {
        logger.debug("retrieve object id: " + objectId);
        FSObject fsObject = retrieveByIdNoThrow(objectId);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found. Requested id is: " + objectId);
        }
        return fsObject;
    }

    FSObject retrieveByIdNoThrow(UUID objectId) {
        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("object_id", objectId);
        return retrieveWorker(sql, params);
    }

    public FSObject retrieveByPath(UUID studyId, String path) {
        FSObject fsObject = retrieveByPathNoThrow(studyId, path);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found - path: '" + path + "'");
        }
        return fsObject;
    }

    public FSObject retrieveByPathNoThrow(UUID studyId, String path) {
        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE path = :path AND study_id = :study_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("path", path)
            .addValue("study_id", studyId);
        return retrieveWorker(sql, params);
    }

    private FSObject retrieveWorker(String sql, MapSqlParameterSource params) {
        try {
            FSObject fsObject = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                new FSObject()
                    .objectId(rs.getObject("object_id", UUID.class))
                    .studyId(rs.getObject("study_id", UUID.class))
                    .objectType(FSObject.FSObjectType.fromLetter(rs.getString("object_type")))
                    .createdDate(rs.getTimestamp("created_date").toInstant())
                    .flightId(rs.getString("flight_id"))
                    .path(rs.getString("path"))
                    .gspath(rs.getString("gspath"))
                    .checksumCrc32c(rs.getString("checksum_crc32c"))
                    .checksumMd5(rs.getString("checksum_md5"))
                    .size(rs.getLong("size"))
                    .mimeType(rs.getString("mime_type"))
                    .description(rs.getString("description")));
            return fsObject;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    public List<FSObject> enumerateDirectory(FSObject dirObject) {
        if (dirObject.getObjectType() != FSObject.FSObjectType.DIRECTORY) {
            throw new InvalidFileSystemObjectTypeException("You can only enumerate directories");
        }

        List<UUID> objectIdList = enumerateDirectoryIds(dirObject);
        List<FSObject> fsObjectList = new ArrayList<>();
        for (UUID objectId : objectIdList) {
            if (objectId != null) {
                FSObject fsObject = retrieve(objectId);
                fsObjectList.add(fsObject);
            }
        }
        return fsObjectList;
    }

    private List<UUID> enumerateDirectoryIds(FSObject dirObject) {
        // Build list of objects ids of object that are in the immediate directory
        // Note that the resulting list has nulls for objects that are on this path
        // but not in this directory.
        try {
            String sql = "SELECT object_id, path FROM fs_object WHERE study_id = :study_id AND path LIKE :pattern";
            String pathPrefix = dirObject.getPath() + "/";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("study_id", dirObject.getStudyId())
                .addValue("pattern", pathPrefix + "%");
            List<UUID> objectIdList = jdbcTemplate.query(
                sql,
                params,
                (rs, rowNum) -> {
                    String objectPath = rs.getString("path");
                    String remaining = StringUtils.removeStart(objectPath, pathPrefix);
                    if (StringUtils.contains(remaining, '/')) {
                        // If the remaining string has a /, that means it is below this directory
                        // and shouldn't be included.
                        return null;
                    }
                    return rs.getObject("object_id", UUID.class);
                });
            return objectIdList;
        } catch (EmptyResultDataAccessException ex) {
            // it is OK if there are no matches
        }

        return Collections.emptyList();
    }

    /**
     * validate ids from a FILEREF or DIRREF column. Used during data ingest
     *
     * @param studyId - study we are checking in
     * @param refIdArray - refIds to check
     * @param objectType - type of ids we are checking
     * @return array of invalid refIds
     */
    public List<String> validateRefIds(UUID studyId, List<String> refIdArray, FSObject.FSObjectType objectType) {
        String sql = "SELECT COUNT(*) AS match_count FROM fs_object " +
            " WHERE study_id = :study_id AND object_type = :object_type" +
            " AND object_id::text = :test_id";

        List<String> invalidRefIds = new ArrayList<>();

        // Brute force, but I really want to be able to generate the list of invalid ids and
        // this is simple. Alternately, could implement a sort-merge join in code here... guk!
        for (String testId : refIdArray) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("test_id", testId)
                .addValue("study_id", studyId)
                .addValue("object_type", objectType.toLetter());

            try {
                Long matchCount = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                    rs.getLong("match_count"));
                if (matchCount == null || matchCount == 0) {
                    invalidRefIds.add(testId);
                }
            } catch (EmptyResultDataAccessException ex) {
                invalidRefIds.add(testId);
            }
        }

        return invalidRefIds;
    }

    public void storeDatasetFileDependencies(UUID datasetId, List<String> refIds) {
        // The ON CONFLICT clause will quietly skip the insert if the
        // (object_id, dataset_id) pair already exists.
        // TODO: We will need to revisit this when we do steward operations for modifying references
        // to files in tabular data. One thing we could do is include the row id and column name
        // of the row/column making the reference. That would make this table a lot bigger and
        // very exact.
        String sql = "INSERT INTO fs_dataset (object_id, dataset_id)" +
            " VALUES (:object_id, :dataset_id)" +
            " ON CONFLICT DO NOTHING";
        for (String refId : refIds) {
            UUID refUUID = UUID.fromString(refId);
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("object_id", refUUID)
                .addValue("dataset_id", datasetId);
            jdbcTemplate.update(sql, params);
        }
    }

    public void deleteDatasetFileDependencies(UUID datasetId) {
        jdbcTemplate.update("DELETE FROM fs_dataset WHERE dataset_id = :dataset_id",
            new MapSqlParameterSource().addValue("dataset_id", datasetId));
    }

    // Make an FSObject with the path filled in. We set it as a directory.
    private FSObject makeDirectory(UUID studyId, String path) {
        return new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.DIRECTORY)
            .path(path);
    }


    String getContainingDirectoryPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length == 1) {
            // We are at the root; no containing directory
            return null;
        }
        int endIndex = pathParts.length - 1;
        return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
    }

}
