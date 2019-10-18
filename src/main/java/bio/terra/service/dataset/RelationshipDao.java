package bio.terra.service.dataset;

import bio.terra.common.DaoKeyHolder;
import bio.terra.common.Table;
import bio.terra.common.Column;
import bio.terra.model.RelationshipTermModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class RelationshipDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public RelationshipDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from DatasetDao
    public void createDatasetRelationships(Dataset dataset) {
        for (DatasetRelationship rel : dataset.getRelationships()) {
            create(rel);
        }
    }

    protected void create(DatasetRelationship datasetRelationship) {
        String sql = "INSERT INTO dataset_relationship " +
                "(name, from_cardinality, to_cardinality, from_table, from_column, to_table, to_column) VALUES " +
                "(:name, :from_cardinality, :to_cardinality, :from_table, :from_column, :to_table, :to_column)";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", datasetRelationship.getName())
                .addValue("from_cardinality", datasetRelationship.getFromCardinality().toString())
                .addValue("to_cardinality", datasetRelationship.getToCardinality().toString())
                .addValue("from_table", datasetRelationship.getFromTable().getId())
                .addValue("from_column", datasetRelationship.getFromColumn().getId())
                .addValue("to_table", datasetRelationship.getToTable().getId())
                .addValue("to_column", datasetRelationship.getToColumn().getId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID relationshipId = keyHolder.getId();
        datasetRelationship.id(relationshipId);
    }

    public void retrieve(Dataset dataset) {
        List<UUID> columnIds = new ArrayList<>();
        dataset.getTables().forEach(table ->
                table.getColumns().forEach(column -> columnIds.add(column.getId())));
        dataset.relationships(
            retrieveDatasetRelationships(columnIds, dataset.getTablesById(), dataset.getAllColumnsById()));
    }

    private List<DatasetRelationship> retrieveDatasetRelationships(
            List<UUID> columnIds,
            Map<UUID, Table> tables,
            Map<UUID, Column> columns) {
        String sql = "SELECT id, name, from_cardinality, to_cardinality, from_table, from_column, to_table, to_column "
                + "FROM dataset_relationship WHERE from_column IN (:columns) OR to_column IN (:columns)";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("columns", columnIds);
        return jdbcTemplate.query(sql, params, (rs, rowNum) ->
                new DatasetRelationship()
                        .id(rs.getObject("id", UUID.class))
                        .name(rs.getString("name"))
                        .fromCardinality(RelationshipTermModel.CardinalityEnum.fromValue(
                                rs.getString("from_cardinality")))
                        .toCardinality(RelationshipTermModel.CardinalityEnum.fromValue(
                                rs.getString("to_cardinality")))
                        .fromTable(tables.get(rs.getObject("from_table", UUID.class)))
                        .fromColumn(columns.get(rs.getObject("from_column", UUID.class)))
                        .toTable(tables.get(rs.getObject("to_table", UUID.class)))
                        .toColumn(columns.get(rs.getObject("to_column", UUID.class))));
    }
}
