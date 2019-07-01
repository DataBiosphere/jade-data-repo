package bio.terra.dao;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.dao.exception.DrDatasetNotFoundException;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.DrDatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class DrDatasetDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DrDatasetTableDao tableDao;
    private final RelationshipDao relationshipDao;
    private final AssetDao assetDao;

    @Autowired
    public DrDatasetDao(NamedParameterJdbcTemplate jdbcTemplate,
                        DrDatasetTableDao tableDao,
                        RelationshipDao relationshipDao,
                        AssetDao assetDao) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableDao = tableDao;
        this.relationshipDao = relationshipDao;
        this.assetDao = assetDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public UUID create(DrDataset dataset) {
        String sql = "INSERT INTO dataset (name, description) VALUES (:name, :description)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", dataset.getName())
                .addValue("description", dataset.getDescription());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID datasetId = keyHolder.getId();
        dataset.id(datasetId);
        dataset.createdDate(keyHolder.getCreatedDate());
        tableDao.createTables(dataset.getId(), dataset.getTables());
        relationshipDao.createDatasetRelationships(dataset);
        assetDao.createAssets(dataset);
        return datasetId;
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

    public DrDataset retrieve(UUID id) {
        DrDatasetSummary summary = retrieveSummaryById(id);
        return retrieveWorker(summary);
    }

    public DrDataset retrieveByName(String name) {
        DrDatasetSummary summary = retrieveSummaryByName(name);
        return retrieveWorker(summary);
    }

    private DrDataset retrieveWorker(DrDatasetSummary summary) {
        DrDataset dataset = null;
        try {
            if (summary != null) {
                dataset = new DrDataset(summary);
                dataset.tables(tableDao.retrieveTables(dataset.getId()));
                relationshipDao.retrieve(dataset);
                assetDao.retrieve(dataset);
            }
            return dataset;
        } catch (EmptyResultDataAccessException ex) {
            throw new CorruptMetadataException("Inconsistent data", ex);
        }
    }

    public DrDatasetSummary retrieveSummaryById(UUID id) {
        try {
            String sql = "SELECT id, name, description, created_date FROM dataset WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new DrDatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DrDatasetNotFoundException("DrDataset not found for id " + id.toString());
        }
    }

    public DrDatasetSummary retrieveSummaryByName(String name) {
        try {
            String sql = "SELECT id, name, description, created_date FROM dataset WHERE name = :name";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("name", name);
            return jdbcTemplate.queryForObject(sql, params, new DrDatasetSummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new DrDatasetNotFoundException("DrDataset not found for name " + name);
        }
    }

    // does not return sub-objects with studies
    public MetadataEnumeration<DrDatasetSummary> enumerate(
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
        String sql = "SELECT id, name, description, created_date FROM dataset " + whereSql +
            DaoUtils.orderByClause(sort, direction) + " OFFSET :offset LIMIT :limit";
        params.addValue("offset", offset).addValue("limit", limit);
        List<DrDatasetSummary> summaries = jdbcTemplate.query(sql, params, new DrDatasetSummaryMapper());

        return new MetadataEnumeration<DrDatasetSummary>()
            .items(summaries)
            .total(total == null ? -1 : total);
    }

    private static class DrDatasetSummaryMapper implements RowMapper<DrDatasetSummary> {
        public DrDatasetSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new DrDatasetSummary()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant());
        }
    }
}
