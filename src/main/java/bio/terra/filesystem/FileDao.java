package bio.terra.filesystem;

import bio.terra.dao.DaoKeyHolder;
import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.FSObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class FileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FileDao");
    private final NamedParameterJdbcTemplate jdbcTemplate;

    private static final String COLUMN_LIST =
        "object_id,study_id,object_type,created_date,path,gspath," +
            "checksum_crc32c,checksum_md5,size,mime_type,description,flight_id";

    @Autowired
    public FileDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Part one of file create. This creates the directory path and file objects.
     * The file object is annotated as "ingesting file", so that it will not be looked up
     * by data repo clients while we are creating it.
     *
     * Note the handling of created_date. We default the created_date to now() so there will always
     * be a proper date in the field. However, on createFileComplete we overwrite it using the value
     * from GCS. That way our created date and the date the client sees from interacting with
     * GCS will be the same.
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public UUID createFileStart(FSObject fileToCreate) {
        if (fileToCreate.getObjectType() != FSObject.FSObjectType.INGESTING_FILE) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }

        // We can think of the path in three parts:
        // 1. The directories that exist; there might be none of these.
        // 2. The directories that do not exist; there might be none of these either.
        //    Once you get to the first directory that does not exist, you know that
        //    no lower directory or file will exist.
        // 3. The file itself.
        //
        // The code below follow those three steps.

        String[] pathParts = StringUtils.split(fileToCreate.getPath(), '/');
        int pathDirectoryLength = pathParts.length - 1;

        // Find the first non-existent directory (if any)
        int index = 0;
        String existingDirectoryPath = "";
        StringBuilder pathBuilder = new StringBuilder();
        for (; index < pathDirectoryLength; index++) {
            pathBuilder.append('/').append(pathParts[index]);
            FSObject fsObject = retrieveFileByPathNoThrow(pathBuilder.toString());
            if (fsObject == null) {
                // We found a non-existent directory
                break;
            }
            existingDirectoryPath = pathBuilder.toString();
        }

        // Create directories from index down (if any)
        // We restart the string builder so it doesn't contain any non-existent
        // directories.
        pathBuilder = new StringBuilder(existingDirectoryPath);
        for (; index < pathDirectoryLength; index++) {
            pathBuilder.append('/').append(pathParts[index]);
            FSObject fsObject = makeObject(fileToCreate.getStudyId(), pathBuilder.toString());
            createObject(fsObject);
        }

        // Finally, create the file.
        return createObject(fileToCreate);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID createFileComplete(FSObject fsObject) {
        FSObject fileToComplete = retrieveFileByIdNoThrow(fsObject.getObjectId());
        if (fileToComplete.getObjectType() != FSObject.FSObjectType.INGESTING_FILE) {
            throw new CorruptMetadataException("Unexpected file system object type");
        }
        if (!StringUtils.equals(fileToComplete.getFlightId(),
            fsObject.getFlightId())) {
            throw new CorruptMetadataException("Unexpected flight id on file");
        }

        String sql = "UPDATE fs_object" +
            " SET object_type = :object_type, gspath = :gspath," +
            " checksum_crc32c = :checksum_crc32c, checksum_md5 = :checksum_md5, " +
            "size = :size, created_date = :created_date" +
            " WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("object_id", fsObject.getObjectId())
            .addValue("object_type", FSObject.FSObjectType.FILE.toLetter())
            .addValue("gspath", fsObject.getGspath())
            .addValue("checksum_crc32c", fsObject.getChecksumCrc32c())
            .addValue("checksum_md5", fsObject.getChecksumMd5())
            .addValue("size", fsObject.getSize())
            .addValue("created_date", Timestamp.from(fsObject.getCreatedDate()));

        jdbcTemplate.update(sql, params);
        return fsObject.getObjectId();
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void createFileCompleteUndo(FSObject fsObject) {
        // Reset to mark the file not preset
        FSObject file = retrieveFileByIdNoThrow(fsObject.getObjectId());
        if (!StringUtils.equals(file.getFlightId(),
            fsObject.getFlightId())) {
            throw new CorruptMetadataException("Unexpected flight id on file");
        }

        if (file.getObjectType() == FSObject.FSObjectType.INGESTING_FILE) {
            return;
        }

        String sql = "UPDATE fs_object SET object_type = :object_type WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("object_id", fsObject.getObjectId())
            .addValue("object_type", FSObject.FSObjectType.INGESTING_FILE.toLetter());
        jdbcTemplate.update(sql, params);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteFileForCreateUndo(UUID objectId, String flightId) {
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            return false;
        }
        switch (fsObject.getObjectType()) {
            case INGESTING_FILE:
                // Only the creating flight can delete the non-present file
                if (!StringUtils.equals(flightId, fsObject.getFlightId())) {
                    throw new InvalidFileSystemObjectTypeException(
                        "Invalid attempt to delete a file being ingested by a different flight");
                }
                break;

            case DIRECTORY:
            case DELETING_FILE:
            case FILE:
            default:
                throw new CorruptMetadataException("Attempt to deleteFileForCreateUndo with bad file object type");
        }

        return deleteFileWorker(fsObject);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteFileStart(UUID objectId, String flightId) {
        // Validate existence and state
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            return false;
        }
        switch (fsObject.getObjectType()) {
            case FILE:
                break;
            case DIRECTORY:
                throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
            case DELETING_FILE:
                if (!StringUtils.equals(fsObject.getFlightId(), flightId)) {
                    throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                }
                break;
            case INGESTING_FILE:
                throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
            default:
                throw new CorruptMetadataException("Unknown file system object type");
        }

        if (hasDatasetReferences(objectId)) {
            throw new FileSystemObjectDependencyException(
                "File is used by at least one dataset and cannot be deleted");
        }

        String sql = "UPDATE fs_object" +
            " SET object_type = :object_type, flight_id = :flight_id" +
            " WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("object_id", fsObject.getObjectId())
            .addValue("object_type", FSObject.FSObjectType.DELETING_FILE.toLetter())
            .addValue("flight_id", flightId);
        jdbcTemplate.update(sql, params);
        return true;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteFileComplete(UUID objectId, String flightId) {
        // Validate existence and state
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            return false;
        }
        switch (fsObject.getObjectType()) {
            case FILE:
                throw new CorruptMetadataException("File is not marked for deletion");
            case DIRECTORY:
                throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
            case DELETING_FILE:
                if (!StringUtils.equals(fsObject.getFlightId(), flightId)) {
                    throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                }
                break;
            case INGESTING_FILE:
                throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
            default:
                throw new CorruptMetadataException("Unknown file system object type");
        }

        return deleteFileWorker(fsObject);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteFileStartUndo(UUID objectId, String flightId) {
        // If we are the ones that put the file in DELETING_FILE state, then we revert it to FILE state.
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            return;
        }
        switch (fsObject.getObjectType()) {
            case DELETING_FILE:
                if (!StringUtils.equals(fsObject.getFlightId(), flightId)) {
                    return;
                }
                break;

            case FILE:
            case DIRECTORY:
            case INGESTING_FILE:
                return;
            default:
                throw new CorruptMetadataException("Unknown file system object type");
        }

        String sql = "UPDATE fs_object" +
            " SET object_type = :object_type" +
            " WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("object_id", fsObject.getObjectId())
            .addValue("object_type", FSObject.FSObjectType.FILE.toLetter());
        jdbcTemplate.update(sql, params);
    }

    // The worker assumes all object type checks have been done by the caller. If verifies that the file is
    // not used in a dataset and performs the delete of the object and any empty parents.
    private boolean deleteFileWorker(FSObject fsObject) {
        if (hasDatasetReferences(fsObject.getObjectId())) {
            throw new FileSystemObjectDependencyException(
                "File is used by at least one dataset and cannot be deleted");
        }
        if (deleteObject(fsObject.getObjectId())) {
            deleteEmptyParents(fsObject.getPath());
        }
        return true;
    }

    void deleteEmptyParents(String path) {
        String parentPath = getContainingDirectoryPath(path);
        if (parentPath == null) {
            // We are at the '/'. Nowhere else to go.
            return;
        }

        String matcher = parentPath + '%';
        String sql = "SELECT COUNT(*) AS match_count FROM fs_object WHERE path LIKE :matcher";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("matcher", matcher);
        Long matchCount  = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
            rs.getLong("match_count"));
        if (matchCount == null) {
            throw new CorruptMetadataException("Match count query returned no data");
        }
        if (matchCount > 1) {
            // There is an object other than directory with the path; that means the directory is not empty
            return;
        }

        FSObject fsObject = retrieveFileByPathNoThrow(parentPath);
        if (hasDatasetReferences(fsObject.getObjectId())) {
            return;
        }

        if (deleteObject(fsObject.getObjectId())) {
            deleteEmptyParents(parentPath);
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

    public FSObject retrieveFile(UUID objectId) {
        logger.debug("retrieve file id: " + objectId);
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File not id: " + objectId);
        }
        return fsObject;
    }

    FSObject retrieveFileByIdNoThrow(UUID objectId) {
        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("object_id", objectId);
        return retrieveWorker(sql, params);
    }

    public FSObject rerieveFileByPath(String path) {
        FSObject fsObject = retrieveFileByPathNoThrow(path);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File not found - path: '" + path + "'");
        }
        return fsObject;
    }

    public FSObject retrieveFileByPathNoThrow(String path) {
        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE path = :path";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("path", path);
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

    /**
     * validate ids from a FILEREF or DIRREF column. Used during data ingest
     *
     * @param studyId - study we are checking in
     * @param refIdArray - refIds to check
     * @param objectType - type of ids we are checking
     * @return array of invalid refIds
     */
    public List<String> validateRefIds(UUID studyId, List<String> refIdArray, FSObject.FSObjectType objectType) {
        String sql = "SELECT object_id, (CASE WHEN object_id IN (:idlist) THEN 1 ELSE 0 ESAC) AS valid" +
            " FROM fs_object WHERE study_id = :study_id AND object_type = :object_type";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("idlist", refIdArray)
            .addValue("study_id", studyId.toString())
            .addValue("object_type", objectType.toLetter());

        try {
            List<String> invalidRefIdList = new ArrayList<>();

            jdbcTemplate.queryForObject(sql, params, (rs, rowNum) -> {
                String objectId = rs.getObject("object_id", UUID.class).toString();
                Integer valid = rs.getInt("valid");
                if (valid == 0) {
                    invalidRefIdList.add(objectId);
                }
                return invalidRefIdList;
            });

            return invalidRefIdList;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    // Make an FSObject with the path filled in. We set it as a directory.
    private FSObject makeObject(UUID studyId, String path) {
        return new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.DIRECTORY)
            .path(path);
    }

    private UUID createObject(FSObject fsObject) {
        logger.debug("create " + fsObject.getObjectType() + ": " + fsObject.getPath());
        String sql = "INSERT INTO fs_object (" +
            "study_id,object_type,flight_id,path,gspath," +
            "checksum_crc32c,checksum_md5,size,mime_type,description)" +
            " VALUES (" +
            ":study_id,:object_type,:flight_id,:path,:gspath," +
            ":checksum_crc32c,:checksum_md5,:size,:mime_type,:description)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("study_id", fsObject.getStudyId())
            .addValue("object_type", fsObject.getObjectType().toLetter())
            .addValue("flight_id", fsObject.getFlightId())
            .addValue("path", fsObject.getPath())
            .addValue("gspath", fsObject.getGspath())
            .addValue("checksum_crc32c", fsObject.getChecksumCrc32c())
            .addValue("checksum_md5", fsObject.getChecksumMd5())
            .addValue("size", fsObject.getSize())
            .addValue("mime_type", fsObject.getMimeType())
            .addValue("description", fsObject.getDescription());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID objectId = keyHolder.getField("object_id", UUID.class);
        Instant createdDate = keyHolder.getCreatedDate();
        fsObject
            .objectId(objectId)
            .createdDate(createdDate);

        return fsObject.getObjectId();
    }

    public String getContainingDirectoryPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length == 1) {
            // We are at the root; no containing directory
            return null;
        }
        int endIndex = pathParts.length - 1;
        return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
    }

}
