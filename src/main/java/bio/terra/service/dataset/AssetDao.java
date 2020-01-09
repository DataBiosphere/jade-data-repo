package bio.terra.service.dataset;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Table;
import bio.terra.common.Column;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class AssetDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public AssetDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    // part of a transaction propagated from DatasetDao
    public List<UUID> createAssets(Dataset dataset) {
        return dataset.getAssetSpecifications()
                .stream()
                .map(assetSpec -> create(assetSpec, dataset.getId()))
                .collect(Collectors.toList());
    }

    public UUID create(AssetSpecification assetSpecification, UUID datasetId) {
        String sql = "INSERT INTO asset_specification (dataset_id, name, root_table_id, root_column_id) " +
                "VALUES (:dataset_id, :name, :root_table_id, :root_column_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("dataset_id", datasetId);
        params.addValue("name", assetSpecification.getName());
        params.addValue("root_table_id", assetSpecification.getRootTable().getTable().getId());
        params.addValue("root_column_id", assetSpecification.getRootColumn().getDatasetColumn().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
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

    public void retrieve(Dataset dataset) {
        dataset.assetSpecifications(retrieveAssetSpecifications(dataset));
    }

    // also retrieves dependent objects
    public List<AssetSpecification> retrieveAssetSpecifications(Dataset dataset) {
        Map<UUID, Table> allTables = dataset.getTablesById();
        Map<UUID, Column> allColumns = dataset.getAllColumnsById();
        Map<UUID, DatasetRelationship> allRelationships = dataset.getRelationshipsById();

        String sql = "SELECT id, name, root_table_id, root_column_id FROM asset_specification WHERE dataset_id = " +
                ":datasetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("datasetId", dataset.getId());

        return jdbcTemplate.query(sql, params, (rs, rowNum) -> {
            UUID specId = rs.getObject("id", UUID.class);
            AssetSpecification spec = new AssetSpecification()
                    .id(specId)
                    .name(rs.getString("name"));
            spec.assetTables(new ArrayList(
                    retrieveAssetTablesAndColumns(
                            spec,
                            rs.getObject("root_table_id", UUID.class),
                            rs.getObject("root_column_id", UUID.class),
                            allTables,
                            allColumns)));
            spec.assetRelationships(retrieveAssetRelationships(spec.getId(), allRelationships));

            return spec;
        });
    }

    // also retrieves columns
    private Collection<AssetTable> retrieveAssetTablesAndColumns(AssetSpecification spec,
                                                                 UUID rootTableId,
                                                                 UUID rootColumnId,
                                                                 Map<UUID, Table> allTables,
                                                                 Map<UUID, Column> allColumns) {
        Map<UUID, AssetTable> tables = new HashMap<>();
        String sql = "SELECT asset_column.id, asset_column.dataset_column_id, dataset_column.table_id " +
                "FROM asset_column " +
                "INNER JOIN dataset_column ON asset_column.dataset_column_id = dataset_column.id " +
                "WHERE asset_id = :assetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("assetId", spec.getId());
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
            // check to see if this table and column are the root values
            if (rootTableId.equals(tableId) && rootColumnId.equals(columnId)) {
                spec.rootTable(assetTable);
                spec.rootColumn(newColumn);
            }
            // add the new column to the asset table object
            assetTable.getColumns().add(newColumn);
        });
        return tables.values();
    }


    private List<AssetRelationship> retrieveAssetRelationships(
            UUID specId,
            Map<UUID, DatasetRelationship> allRelationships) {
        String sql = "SELECT id, relationship_id FROM asset_relationship WHERE asset_id = :assetId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("assetId", specId);
        return jdbcTemplate.query(sql, params, (rs, rowNum) ->
                new AssetRelationship()
                        .id(rs.getObject("id", UUID.class))
                        .datasetRelationship(allRelationships.get(
                                rs.getObject("relationship_id", UUID.class))));
    }

    public boolean delete(UUID assetId) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM asset_specification WHERE id = :assetId CASCADE",
            new MapSqlParameterSource().addValue("id", assetId));
        return rowsAffected > 0;
    }
}
