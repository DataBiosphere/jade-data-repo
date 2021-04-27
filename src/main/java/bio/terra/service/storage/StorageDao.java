package bio.terra.service.storage;
import bio.terra.model.StorageResourceModel;
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
import java.util.List;

@Repository
public class StorageDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private static final String STORAGE_COLUMNS = "id, dataset_id, cloud_platform, " +
        "cloud_resource, region";
    private static final String SQL_GET = "SELECT " + STORAGE_COLUMNS +
        "FROM storage_resource WHERE id = :id";

    @Autowired
    public StorageDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public List<StorageResourceModel> getStorageResourcesByDatasetId(UUID datasetId) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dataset_id", datasetId);
            return jdbcTemplate.query(SQL_GET, params, new StorageDao.StorageResourceMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StorageResourceNotFoundException("Storage resource not found for dataset: " + datasetId.toString());
        }
    }

    // TODO: update based on the open api schema
    private static class StorageResourceMapper implements RowMapper<StorageResourceModel> {
        public StorageResourceModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            String profileId = rs.getObject("id", UUID.class).toString();
            return new StorageResourceModel()
                .id(profileId)
                .datasetId(rs.getObject("dataset_id", UUID.class).toString())
                .cloudPlatform(StorageResourceModel.CloudPlatformEnum.valueOf(rs.getString("cloud_platform")))
                .cloudResource(StorageResourceModel.CloudResourceEnum.valueOf(rs.getString("cloud_resource")))
                .region(rs.getString("region"));
        }
    }

}
