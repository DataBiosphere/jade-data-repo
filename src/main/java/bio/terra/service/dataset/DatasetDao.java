package bio.terra.service.dataset;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.DaoUtils;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.common.MetadataEnumeration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class DatasetDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Connection connection;
    private final DatasetTableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    @Autowired
    public DatasetDao(DataRepoJdbcConfiguration jdbcConfiguration,
                    DatasetTableDao tableDao,
                    RelationshipDao relationshipDao,
                    AssetDao assetDao) throws SQLException {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
        connection = jdbcConfiguration.getDataSource().getConnection();
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(Dataset dataset) {
        try {
            String sql = "INSERT INTO dataset (name, description, default_profile_id, additional_profile_ids) VALUES " +
                "(:name, :description, :default_profile_id, :additional_profile_ids)";
            Array additionalProfileIds = DaoUtils.createSqlUUIDArray(connection, dataset.getAdditionalProfileIds());
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", dataset.getName())
                .addValue("description", dataset.getDescription())
                .addValue("default_profile_id", dataset.getDefaultProfileId())
                .addValue("additional_profile_ids", additionalProfileIds);
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            UUID datasetId = keyHolder.getId();
            dataset.id(datasetId);
            dataset.createdDate(keyHolder.getCreatedDate());
            tableDao.createTables(dataset.getId(), dataset.getTables());
            relationshipDao.createDatasetRelationships(dataset);
            assetDao.createAssets(dataset);
            return datasetId;
        } catch (DuplicateKeyException | SQLException e) {
            throw new InvalidDatasetException("Cannot create dataset: " + dataset.getName(), e);
        }
    }

    @Transactional
    public boolean delete(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE id = :id",
                new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    @Transactional
    public boolean deleteByName(String datasetName) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM dataset WHERE name = :name",
                new MapSqlParameterSource().addValue("name", datasetName));
        return rowsAffected > 0;
    }

    public Dataset retrieve(UUID id) {
        DatasetSummary summary = retrieveSummaryById(id);
        return retrieveWorker(summary);
    }

    public Dataset retrieveByName(String name) {
        DatasetSummary summary = retrieveSummaryByName(name);
        return retrieveWorker(summary);
    }

    private Dataset retrieveWorker(DatasetSummary summary) {
        Dataset dataset = null;
        try {
            if (summary != null) {
                dataset = new Dataset(summary);
                dataset.tables(tableDao.retrieveTables(dataset.getId()));
                relationshipDao.retrieve(dataset);
                assetDao.retrieve(dataset);
            }
            return dataset;
        } catch (EmptyResultDataAccessException ex) {
            throw new CorruptMetadataException("Inconsistent data", ex);
        }
    }

    public DatasetSummary retrieveSummaryById(UUID id) {
        try {
            String sql = "SELECT id, name, description, default_profile_id, additional_profile_ids, created_date " +
                " FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for id " + id.toString());
        }
    }

    public DatasetSummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT id, name, description, default_profile_id, additional_profile_ids, created_date " +
                " FROM dataset WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new DatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DatasetNotFoundException("Dataset not found for name " + name);
        }
    }

    // does not return sub-objects with datasets
    public MetadataEnumeration<DatasetSummary> enumerate(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> accessibleDatasetIds
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        DaoUtils.addAuthzIdsClause(accessibleDatasetIds, params, whereClauses);

        // get total count of objects
        String countSql = "SELECT count(id) AS total FROM dataset WHERE " +
            StringUtils.join(whereClauses, " AND ");
        Integer total = jdbcTemplate.queryForObject(countSql, params, Integer.class);

        // add the filter to the clause to get the actual items
        DaoUtils.addFilterClause(filter, params, whereClauses);
        String whereSql = "";
        if (!whereClauses.isEmpty()) {
            whereSql = " WHERE " + StringUtils.join(whereClauses, " AND ");
        }
        String sql = "SELECT id, name, description, created_date, default_profile_id, additional_profile_ids " +
            " FROM dataset " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<DatasetSummary> summaries = jdbcTemplate.query(sql, params, new DatasetSummaryMapper());

        return new MetadataEnumeration<DatasetSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    private static class DatasetSummaryMapper implements RowMapper<DatasetSummary> {
        public DatasetSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DatasetSummary()
                    .id(rs.getObject("id", UUID.class))
                    .name(rs.getString("name"))
                    .description(rs.getString("description"))
                    .defaultProfileId(rs.getObject("default_profile_id", UUID.class))
                    .additionalProfileIds(DaoUtils.getUUIDList(rs, "additional_profile_ids"))
                    .createdDate(rs.getTimestamp("created_date").toInstant());
        }
    }
}
