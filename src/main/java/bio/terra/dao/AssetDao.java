package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetRelationship;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyRelationship;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class AssetDao extends MetaDao<AssetSpecification> {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public AssetDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    // part of a transaction propagated from StudyDao
    public List<UUID> createAssets(Study study) {
        return study.getAssetSpecifications()
                .stream()
                .map(assetSpec -> create(assetSpec, study))
                .collect(Collectors.toList());
    }

    private UUID create(AssetSpecification assetSpecification, Study study) {
        String sql = "INSERT INTO asset_specification (study_id, name, root_table_id) " +
                "VALUES (:study_id, :name, :root_table_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("study_id", study.getId());
        params.addValue("name", assetSpecification.getName());
        params.addValue("root_table_id", assetSpecification.getRootTable().getStudyTable().getId());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID assetSpecId = getIdKey(keyHolder);
        assetSpecification.setId(assetSpecId);

        createAssetColumns(assetSpecification);
        createAssetRelationships(assetSpecification);
        return assetSpecId;
    }

    private void createAssetColumns(AssetSpecification assetSpec) {
        assetSpec.getAssetTables().forEach(assetTable -> {
            assetTable.getColumns().forEach(assetCol -> {
                String sql = "INSERT INTO asset_column (asset_id, study_column_id) " +
                        "VALUES (:asset_id, :study_column_id)";
                MapSqlParameterSource params = new MapSqlParameterSource();
                params.addValue("asset_id", assetSpec.getId());
                params.addValue("study_column_id", assetCol.getStudyColumn().getId());
                KeyHolder keyHolder = new GeneratedKeyHolder();
                jdbcTemplate.update(sql, params, keyHolder);
                UUID assetColumnId = getIdKey(keyHolder);
                assetCol.setId(assetColumnId);
            });
        });
    }

    private void createAssetRelationships(AssetSpecification assetSpec) {
        assetSpec.getAssetRelationships().forEach(assetRel -> {
            String sql = "INSERT INTO asset_relationship (asset_id, relationship_id) " +
                    "VALUES (:asset_id, :relationship_id)";
            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("asset_id", assetSpec.getId());
            params.addValue("relationship_id", assetRel.getStudyRelationship().getId());
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            UUID assetRelId = getIdKey(keyHolder);
            assetRel.setId(assetRelId);
        });
    }

    private static final class AssetSpecificationMapper implements RowMapper<AssetSpecification> {
        private Map<UUID, UUID> specIdToRootTableId = new HashMap<>();

        public Map<UUID, UUID> getSpecIdToRootTableId() { return specIdToRootTableId; }

        public AssetSpecification mapRow(ResultSet rs, int rowNum) throws SQLException {
            UUID specId = UUID.fromString(rs.getString("id"));
            specIdToRootTableId.put(specId, UUID.fromString(rs.getString("root_table_id")));
            return new AssetSpecification()
                    .setId(specId)
                    .setName(rs.getString("name"));
        }
    }

    private static final class AssetColumnMapper implements RowMapper<AssetColumn> {
        private Map<UUID, StudyTableColumn> allColumns;

        AssetColumnMapper(Map<UUID, StudyTableColumn> allColumns) { this.allColumns = allColumns; }

        public AssetColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new AssetColumn()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setStudyColumn(allColumns.get(UUID.fromString(rs.getString("study_column_id"))));
        }
    }

    private static final class AssetRelationshipMapper implements RowMapper<AssetRelationship> {
        private Map<UUID, StudyRelationship> allRelationships;

        AssetRelationshipMapper(Map<UUID, StudyRelationship> allRelationships) {
            this.allRelationships = allRelationships;
        }

        public AssetRelationship mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new AssetRelationship()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setStudyRelationship(allRelationships.get(
                            UUID.fromString(rs.getString("relationship_id"))));
        }
    }

    //    @Override
    public void retrieve(Study study) {
        study.setAssetSpecifications(retrieveAssetSpecifications(study));
    }

    // also retrieves dependent objects
    public List<AssetSpecification> retrieveAssetSpecifications(Study study) {
        AssetSpecificationMapper rsMapper = new AssetSpecificationMapper();
        List<AssetSpecification> specs = jdbcTemplate.query(
                "SELECT id, name, root_table_id FROM asset_specification WHERE study_id = :study_id",
                new MapSqlParameterSource().addValue("study_id", study.getId()),
                rsMapper);
        Map<UUID, StudyTable> allTables = study.getTablesById();
        Map<UUID, StudyTableColumn> allColumns = study.getAllColumnsById();
        Map<UUID, StudyRelationship> allRelationships = study.getRelationshipsById();
        Map<UUID, UUID> rootTableIdMap = rsMapper.getSpecIdToRootTableId();
        specs.forEach(spec -> {
            spec.setAssetTables(new ArrayList(
                    retrieveAssetTablesAndColumns(spec, rootTableIdMap.get(spec.getId()), allTables, allColumns)));
            retrieveAssetRelationships(spec, allRelationships);
        });
        return specs;
    }

    // also retrieves columns
    private Collection<AssetTable> retrieveAssetTablesAndColumns(AssetSpecification spec,
                                                                 UUID rootTableId,
                                                                 Map<UUID, StudyTable> allTables,
                                                                 Map<UUID, StudyTableColumn> allColumns) {
        Map<UUID, AssetTable> tables = new HashMap<>();
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
                "SELECT asset_column.id, asset_column.study_column_id, study_column.table_id " +
                        "FROM asset_column " +
                        "INNER JOIN study_column ON asset_column.study_column_id = study_column.id " +
                        "WHERE asset_id = :assetId",
                new MapSqlParameterSource().addValue("assetId", spec.getId()));
        results
                .forEach(rs -> {
                    UUID tableId = UUID.fromString(rs.get("table_id").toString());
                    if (!tables.containsKey(tableId)) {
                        tables.put(tableId, new AssetTable().setStudyTable(allTables.get(tableId)));
                    }
                    AssetTable newAssetTable = tables.get(tableId);
                    if (spec.getRootTable() == null && rootTableId.equals(tableId)) {
                        spec.setRootTable(newAssetTable);
                    }
                    AssetColumn newColumn = new AssetColumn()
                            .setId(UUID.fromString(rs.get("id").toString()))
                            .setStudyColumn(allColumns.get(rs.get("study_column_id").toString()));
                    newAssetTable.getColumns().add(newColumn);
                });
        return tables.values();
    }


    private void retrieveAssetRelationships(AssetSpecification spec, Map<UUID, StudyRelationship> allRelationships) {
        List<AssetRelationship> relationships = jdbcTemplate.query(
                "SELECT id, relationship_id FROM asset_relationship WHERE asset_id = :assetId",
                new MapSqlParameterSource().addValue("assetId", spec.getId()),
                new AssetRelationshipMapper(allRelationships));
        spec.setAssetRelationships(relationships);
    }
}
