package bio.terra.service.dataset.dao;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.Column;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Relationship;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidAssetException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class AssetDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public AssetDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    // Creates assets as part of create dataset
    void createAssets(Dataset dataset) {
        for (AssetSpecification assetSpec : dataset.getAssetSpecifications()) {
            createAsset(assetSpec, dataset.getId());
        }
    }

    // Create a single asset
    UUID createAsset(AssetSpecification assetSpecification, UUID datasetId) {
        String sql = "INSERT INTO asset_specification (dataset_id, name, root_table_id, root_column_id) " +
            "VALUES (:dataset_id, :name, :root_table_id, :root_column_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("dataset_id", datasetId);
        params.addValue("name", assetSpecification.getName());
        params.addValue("root_table_id", assetSpecification.getRootTable().getTable().getId());
        params.addValue("root_column_id", assetSpecification.getRootColumn().getDatasetColumn().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        try {
            jdbcTemplate.update(sql, params, keyHolder);
        } catch (DuplicateKeyException e) {
            throw new InvalidAssetException("Asset name already exists: " + assetSpecification.getName(), e);
        }

        UUID assetSpecId = keyHolder.getId();
        assetSpecification.id(assetSpecId);

        createAssetColumns(assetSpecification);
        createAssetRelationships(assetSpecification);
        return assetSpecId;
    }

    private void createAssetColumns(AssetSpecification assetSpec) {
        assetSpec.getAssetTables().forEach(assetTable -> {
            assetTable.getColumns().forEach(assetCol -> {
                String sql = "INSERT INTO asset_column (asset_id, dataset_column_id) " +
                    "VALUES (:asset_id, :dataset_column_id)";
                MapSqlParameterSource params = new MapSqlParameterSource();
                params.addValue("asset_id", assetSpec.getId());
                params.addValue("dataset_column_id", assetCol.getDatasetColumn().getId());
                DaoKeyHolder keyHolder = new DaoKeyHolder();
                jdbcTemplate.update(sql, params, keyHolder);
                UUID assetColumnId = keyHolder.getId();
                assetCol.id(assetColumnId);
            });
        });
    }

    private void createAssetRelationships(AssetSpecification assetSpec) {
        assetSpec.getAssetRelationships().forEach(assetRel -> {
            String sql = "INSERT INTO asset_relationship (asset_id, relationship_id) " +
                "VALUES (:asset_id, :relationship_id)";
            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("asset_id", assetSpec.getId());
            params.addValue("relationship_id", assetRel.getDatasetRelationship().getId());
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            UUID assetRelId = keyHolder.getId();
            assetRel.id(assetRelId);
        });
    }

    void retrieve(Dataset dataset) {
        dataset.assetSpecifications(retrieveAssetSpecifications(dataset));
    }

    // also retrieves dependent objects
    private List<AssetSpecification> retrieveAssetSpecifications(Dataset dataset) {
        final Map<UUID, DatasetTable> allTables = dataset.getTablesById();
        final Map<UUID, Column> allColumns = dataset.getAllColumnsById();
        final Map<UUID, Relationship> allRelationships = dataset.getRelationshipsById();

        String sql = "SELECT id, name, root_table_id, root_column_id FROM asset_specification WHERE dataset_id = " +
            ":datasetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("datasetId", dataset.getId());

        return jdbcTemplate.query(sql, params, (rs, rowNum) -> {
            UUID specId = rs.getObject("id", UUID.class);

            // Build a map of asset tables indexed by table id
            Map<UUID, AssetTable> assetTables = retrieveAssetTablesAndColumns(
                specId,
                allTables,
                allColumns);

            // Locate the root table and column
            AssetTable rootTable = assetTables.get(rs.getObject("root_table_id", UUID.class));
            UUID rootColumnId = rs.getObject("root_column_id", UUID.class);
            AssetColumn rootColumn = null;
            for (AssetColumn assetColumn : rootTable.getColumns()) {
                if (rootColumnId.equals(assetColumn.getDatasetColumn().getId())) {
                    rootColumn = assetColumn;
                }
            }

            return new AssetSpecification()
                .id(specId)
                .name(rs.getString("name"))
                .rootColumn(rootColumn)
                .rootTable(rootTable)
                .assetTables(new ArrayList<AssetTable>(assetTables.values()))
                .assetRelationships(retrieveAssetRelationships(specId, allRelationships));
        });
    }

    // also retrieves columns
    private Map<UUID, AssetTable> retrieveAssetTablesAndColumns(UUID specId,
                                                                Map<UUID, DatasetTable> allTables,
                                                                Map<UUID, Column> allColumns) {
        Map<UUID, AssetTable> tables = new HashMap<>();
        String sql = "SELECT asset_column.id, asset_column.dataset_column_id, dataset_column.table_id " +
            "FROM asset_column " +
            "INNER JOIN dataset_column ON asset_column.dataset_column_id = dataset_column.id " +
            "WHERE asset_id = :assetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("assetId", specId);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, params);
        results.forEach(rs -> {
            UUID tableId = (UUID) rs.get("table_id");
            UUID columnId = (UUID) rs.get("dataset_column_id");
            if (!tables.containsKey(tableId)) {
                tables.put(tableId, new AssetTable().datasetTable(allTables.get(tableId)));
            }
            AssetTable assetTable = tables.get(tableId);
            AssetColumn newColumn = new AssetColumn()
                .id((UUID) rs.get("id"))
                .datasetColumn(allColumns.get(columnId));
            // add the new column to the asset table object
            assetTable.getColumns().add(newColumn);
        });
        return tables;
    }


    private List<AssetRelationship> retrieveAssetRelationships(
        UUID specId,
        Map<UUID, Relationship> allRelationships) {
        String sql = "SELECT id, relationship_id FROM asset_relationship WHERE asset_id = :assetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("assetId", specId);
        return jdbcTemplate.query(sql, params, (rs, rowNum) ->
            new AssetRelationship()
                .id(rs.getObject("id", UUID.class))
                .datasetRelationship(allRelationships.get(
                    rs.getObject("relationship_id", UUID.class))));
    }

    public boolean deleteAsset(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM asset_specification WHERE id = :id ",
            new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }
}
