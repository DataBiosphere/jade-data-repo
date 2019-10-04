package bio.terra.service.resourcemanagement;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.service.resourcemanagement.exception.DataProjectNotFoundException;
import bio.terra.service.snapshot.SnapshotDataProjectSummary;
import bio.terra.service.dataset.DatasetDataProjectSummary;
import bio.terra.common.DaoKeyHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@Repository
public class DataProjectDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DataProjectDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    public UUID createDatasetDataProject(DatasetDataProjectSummary datasetDataProjectSummary) {
        String sql = "INSERT INTO dataset_data_project (dataset_id, project_resource_id) VALUES " +
            "(:dataset_id, :project_resource_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", datasetDataProjectSummary.getDatasetId())
            .addValue("project_resource_id", datasetDataProjectSummary.getProjectResourceId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    public DatasetDataProjectSummary retrieveDatasetDataProject(UUID datasetId) {
        try {
            String sql = "SELECT id, dataset_id, project_resource_id FROM dataset_data_project " +
                " WHERE dataset_id = :dataset_id";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dataset_id", datasetId);
            return jdbcTemplate.queryForObject(sql, params, new DatasetDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException("Dataset data project not found for: " + datasetId);
        }
    }

    public boolean deleteDatasetDataProject(UUID id) {
        String sql = "DELETE FROM dataset_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class DatasetDataProjectSummaryMapper implements RowMapper<DatasetDataProjectSummary> {
        public DatasetDataProjectSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DatasetDataProjectSummary()
                .id(rs.getObject("id", UUID.class))
                .datasetId(rs.getObject("dataset_id", UUID.class))
                .projectResourceId(rs.getObject("project_resource_id", UUID.class));
        }
    }

    public UUID createSnapshotDataProject(SnapshotDataProjectSummary snapshotDataProjectSummary) {
        String sql = "INSERT INTO snapshot_data_project (snapshot_id, project_resource_id) VALUES " +
            "(:snapshot_id, :project_resource_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("snapshot_id", snapshotDataProjectSummary.getSnapshotId())
            .addValue("project_resource_id", snapshotDataProjectSummary.getProjectResourceId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

    public SnapshotDataProjectSummary retrieveSnapshotDataProject(UUID snapshotId) {
        try {
            String sql = "SELECT id, snapshot_id, project_resource_id FROM snapshot_data_project " +
                "WHERE snapshot_id = :snapshot_id";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("snapshot_id", snapshotId);
            return jdbcTemplate.queryForObject(sql, params, new SnapshotDataProjectSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DataProjectNotFoundException("Snapshot data project not found for: " + snapshotId);
        }
    }

    public boolean deleteSnapshotDataProject(UUID id) {
        String sql = "DELETE FROM snapshot_data_project WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
        int rowsAffected = jdbcTemplate.update(sql, params);
        return rowsAffected > 0;
    }

    private static class SnapshotDataProjectSummaryMapper implements RowMapper<SnapshotDataProjectSummary> {
        public SnapshotDataProjectSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new SnapshotDataProjectSummary()
                .id(rs.getObject("id", UUID.class))
                .snapshotId(rs.getObject("snapshot_id", UUID.class))
                .projectResourceId(rs.getObject("project_resource_id", UUID.class));
        }
    }
}
