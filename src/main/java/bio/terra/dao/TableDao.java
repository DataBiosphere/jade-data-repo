package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.UUID;

@Repository
public class TableDao extends MetaDao<StudyTable> {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public TableDao(DataRepoJdbcConfiguration jdbcConfiguration) {
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    void createStudyTables(Study study) {
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

    void createStudyColumns(UUID tableId, Collection<StudyTableColumn> columns) {
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


}
