package bio.terra.dao;

import bio.terra.dao.exception.FileSystemObjectNotFoundException;
import bio.terra.metadata.FSObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/*
fs_object table:
 object_id    | uuid                     |           | not null | gen_random_uuid()
 study_id     | uuid                     |           | not null |
 object_type  | character(1)             |           | not null |
 created_date | timestamp with time zone |           | not null | now()
 path         | text                     |           | not null |
 gspath       | text                     |           |          |
 checksum     | text                     |           | not null |
 size         | bigint                   |           |          |
 mime_type    | text                     |           |          |
 description  | text                     |           |          |

 object_id,study_id,object_type,created_date,path,gspath,checksum,size,mime_type,description

 */

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

    @Transactional(propagation = Propagation.REQUIRED)
    public void create() {
        // <<< YOU ARE HERE >>>
    }

    public FSObject retrieveFile(UUID objectId) {
        logger.debug("retrieve file id: " + objectId);

        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("object_id", objectId);
        FSObject fsObject = retrieveWorker(sql, params);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File not found - id: " + objectId);
        }
        return fsObject;
    }

    public FSObject rerieveFileByPath(String path) {
        String sql = "SELECT " + COLUMN_LIST + " FROM fs_object WHERE path = :path";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("path", path);
        FSObject fsObject = retrieveWorker(sql, params);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File not found - path: '" + path + "'");
        }
        return fsObject;
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

}
