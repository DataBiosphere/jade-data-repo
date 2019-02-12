package bio.terra.dao;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public class TableDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public TableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // part of a transaction propagated from StudyDao
    public void createStudyTables(Study study) {
        String sql = "INSERT INTO study_table (name, study_id) VALUES (:name, :study_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("study_id", study.getId());
        UUIDHolder keyHolder = new UUIDHolder();
        for (StudyTable table : study.getTables()) {
            params.addValue("name", table.getName());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID tableId = keyHolder.getId();
            table.setId(tableId);
            createStudyColumns(tableId, table.getColumns());
        }
    }

    protected void createStudyColumns(UUID tableId, Collection<StudyTableColumn> columns) {
        String sql = "INSERT INTO study_column (table_id, name, type) VALUES (:table_id, :name, :type)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("table_id", tableId);
        UUIDHolder keyHolder = new UUIDHolder();
        for (StudyTableColumn column : columns) {
            // TODO: feedback please. this is reusing the same param object and just overriding the name and type
            // values. is this a bad idea? should i be creating a new MapSqlParameterSource every iteration?
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID columnId = keyHolder.getId();
            column.setId(columnId);
        }
    }

    public void retrieve(Study study) {
        List<StudyTable> tables = retrieveStudyTables(study.getId());
        study.setTables(tables);
    }

    // also retrieves columns
    private List<StudyTable> retrieveStudyTables(UUID studyId) {
        String sql = "SELECT id, name FROM study_table WHERE study_id = :studyId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("studyId", studyId);
        List<StudyTable> tables = jdbcTemplate.query(sql, params, (rs, rowNum) ->
                new StudyTable()
                        .setId(UUID.fromString(rs.getString("id")))
                        .setName(rs.getString("name")));
        tables.forEach(studyTable -> studyTable.setColumns(retrieveStudyTableColumns(studyTable)));
        return tables;
    }

    private List<StudyTableColumn> retrieveStudyTableColumns(StudyTable table) {
        String sql = "SELECT id, name, type FROM study_column WHERE table_id = :tableId";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("tableId", table.getId());
        List<StudyTableColumn> columns = jdbcTemplate.query(sql, params, (rs, rowNum) ->
                new StudyTableColumn()
                        .setId(UUID.fromString(rs.getString("id")))
                        .setName(rs.getString("name"))
                        .setType(rs.getString("type"))
                        .setInTable(table));

        return columns;
    }
}
