package bio.terra.dao;

import bio.terra.metadata.Study;
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
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public class TableDao extends MetaDao<StudyTable> {

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
        KeyHolder keyHolder = new GeneratedKeyHolder();
        for (StudyTable table : study.getTables()) {
            params.addValue("name", table.getName());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID tableId = getIdKey(keyHolder);
            table.setId(tableId);
            createStudyColumns(tableId, table.getColumns());
        }
    }

    protected void createStudyColumns(UUID tableId, Collection<StudyTableColumn> columns) {
        String sql = "INSERT INTO study_column (table_id, name, type) VALUES (:table_id, :name, :type)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("table_id", tableId);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        for (StudyTableColumn column : columns) {
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID columnId = getIdKey(keyHolder);
            column.setId(columnId);
        }
    }

    private static final class StudyTableMapper implements RowMapper<StudyTable> {
        public StudyTable mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudyTable()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setName(rs.getString("name"));
        }
    }

    private static final class StudyTableColumnMapper implements RowMapper<StudyTableColumn> {
        public StudyTableColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudyTableColumn()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setName(rs.getString("name"))
                    .setType(rs.getString("type"));
        }
    }

    //    @Override
    public void retrieve(Study study) {
        List<StudyTable> tables = retrieveStudyTables(study.getId());
        study.setTables(tables);
    }

    private List<StudyTable> retrieveStudyTables(UUID studyId) {
        List<StudyTable> tables = jdbcTemplate.query(
                "SELECT id, name FROM study_table WHERE study_id = :studyId",
                new MapSqlParameterSource().addValue("studyId", studyId),
                new StudyTableMapper());
        tables.stream().forEach(studyTable -> studyTable.setColumns(retrieveStudyTableColumns(studyTable.getId())));
        return tables;
    }

    private List<StudyTableColumn> retrieveStudyTableColumns(UUID tableId) {
        List<StudyTableColumn> columns = jdbcTemplate.query(
                "SELECT id, name, type FROM study_column WHERE table_id = :tableId",
                new MapSqlParameterSource().addValue("tableId", tableId),
                new StudyTableColumnMapper());

        return columns;
    }
}
