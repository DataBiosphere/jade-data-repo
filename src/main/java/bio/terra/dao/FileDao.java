package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.FileSystemObjectAlreadyExistsException;
import bio.terra.dao.exception.FileSystemObjectDependencyException;
import bio.terra.dao.exception.FileSystemObjectNotFoundException;
import bio.terra.dao.exception.InvalidFileSystemObjectTypeException;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class FileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.dao.DatasetDao");
    private final NamedParameterJdbcTemplate jdbcTemplate;

    private static final String COLUMN_LIST =
        "object_id,study_id,object_type,created_date,path,gspath,checksum,size,mime_type,description";

    @Autowired
    public FileDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Part one of file create. This creates the directory path and file objects.
     * The file object is annotated as not present, so that it will not be looked up
     * by data repo clients while we are creating it.
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public UUID createFileStart(FSObject fileToCreate) {
        if (fileToCreate.getObjectType() != FSObject.FSObjectType.FILE_NOT_PRESENT) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }
        String[] pathParts = StringUtils.split(fileToCreate.getPath(), '/');

        List<FSObject> createList = new ArrayList<>();

        // Walk down the path, one directory at a time. At the first non-existent directory,
        // construct and collect partial FSObjects.
        String currentPath = "";
        boolean finding = true;
        for (String part : pathParts) {
            currentPath = currentPath + "/" + part;
            if (finding) {
                FSObject fsObject = retrieveFileByPathNoThrow(currentPath);
                if (fsObject == null) {
                    finding = false; // now we are creating
                }
            }
            if (!finding) {
                FSObject fsObject = makeObject(fileToCreate.getStudyId(), currentPath);
                createList.add(fsObject);
            }
        }

        // If the object list is empty, then we have a duplicate path.
        // Otherwise, we replace the last element of the path (the file object)
        // with the incoming file object and create the objects.
        int lastIndex = createList.size() - 1;
        if (lastIndex == -1) {
            throw new FileSystemObjectAlreadyExistsException("Path already exists: " + fileToCreate.getPath());
        }
        createList.set(lastIndex, fileToCreate);

        UUID id = null;  // set null to avoid warning, but due to the check above, we never return null
        for (FSObject fsObject : createList) {
            id = createObject(fsObject);
        }
        return id;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID createFileComplete(FSObject fsObject) {
        FSObject fileNotPresent = retrieveFileByIdNoThrow(fsObject.getObjectId());
        if (fileNotPresent.getObjectType() != FSObject.FSObjectType.FILE_NOT_PRESENT) {
            throw new CorruptMetadataException("Unexpected file system object type");
        }
        if (!StringUtils.equals(fileNotPresent.getCreatingFlightId(),
            fsObject.getCreatingFlightId())) {
            throw new CorruptMetadataException("Unexpected file id on file");
        }

        String sql = "UPDATE fs_object" +
            " SET object_type = :object_type, gspath = :gspath, checksum = :checksum, size = :size" +
            " WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("object_id", fsObject.getObjectId())
            .addValue("object_type", FSObject.FSObjectType.FILE.toLetter())
            .addValue("gspath", fsObject.getGspath())
            .addValue("checksum", fsObject.getChecksum())
            .addValue("size", fsObject.getSize());
        jdbcTemplate.update(sql, params);
        return fsObject.getObjectId();
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteFile(UUID objectId) {
        FSObject fsObject = retrieveFileByIdNoThrow(objectId);
        if (fsObject == null) {
            return false;
        }
        if (fsObject.getObjectType() == FSObject.FSObjectType.DIRECTORY) {
            throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
        }
        if (hasDatasetReferences(objectId)) {
            throw new FileSystemObjectDependencyException(
                "File is used by at least one dataset and cannot be deleted");
        }
        if (deleteObject(objectId)) {
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

    private FSObject retrieveFileByIdNoThrow(UUID objectId) {
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
                    .path(rs.getString("path"))
                    .gspath(rs.getString("gspath"))
                    .checksum(rs.getString("checksum"))
                    .size(rs.getLong("size"))
                    .mimeType(rs.getString("mime_type"))
                    .description(rs.getString("description")));
            return fsObject;
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
            "study_id,object_type,path,gspath,checksum,size,mime_type,description)" +
            " VALUES (" +
            ":study_id,:object_type,:path,:gspath,:checksum,:size,:mime_type,:description)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("study_id", fsObject.getStudyId())
            .addValue("object_type", fsObject.getObjectType().toLetter())
            .addValue("path", fsObject.getPath())
            .addValue("gspath", fsObject.getGspath())
            .addValue("checksum", fsObject.getChecksum())
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

    // TODO: File path manipulation methods; maybe move to a Util class?
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
