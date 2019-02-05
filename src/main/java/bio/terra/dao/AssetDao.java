package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.util.List;
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
        params.addValue("root_table_id", assetSpecification.getRootTable().getId());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID assetSpecId = getIdKey(keyHolder);
        assetSpecification.setId(assetSpecId);

        createAssetColumns(assetSpecification);
        createAssetRelationships(assetSpecification);
        return assetSpecId;
    }

    private void createAssetColumns(AssetSpecification assetSpec) {
        assetSpec.getAssetColumns().forEach(assetCol -> {
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

//    @Override
//    public AssetSpecification retrieve(UUID id) {
//        String sql = "SELECT * FROM asset_specification WHERE id = (id)";
//        MapSqlParameterSource params = new MapSqlParameterSource();
//        params.addValue("id", id);
//        return jdbcTemplate.queryForObject(sql, params, AssetSpecification.class);
//    }

}
