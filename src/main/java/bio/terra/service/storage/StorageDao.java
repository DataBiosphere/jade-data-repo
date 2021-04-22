package bio.terra.service.storage;

import bio.terra.service.storage.exception.StorageResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@Repository
public class StorageDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private static final String STORAGE_COLUMNS = "id, cloud_platform, region";
    private static final String SQL_GET = "SELECT " + STORAGE_COLUMNS +
        "FROM storage_resource WHERE id = :id";

    @Autowired
    public StorageDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public StorageResourceModel getStorageResourceById(UUID id) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id);
            return jdbcTemplate.queryForObject(SQL_GET, params, new StorageDao.StorageResourceMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StorageResourceNotFoundException("Storage resource not found for id: " + id.toString());
        }
    }

    // TODO: update based on the open api schema
    private static class StorageResourceMapper implements RowMapper<StorageResourceModel> {
        public StorageResourceModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            String profileId = rs.getObject("id", UUID.class).toString();
            return StorageResourceModel()
                .id(profileId)
                .cloudProvider(rs.getString("cloud_provider"))
                .region(rs.getString("region"));
        }
    }

}
