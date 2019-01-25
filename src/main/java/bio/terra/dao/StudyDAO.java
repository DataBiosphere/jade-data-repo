package bio.terra.dao;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class StudyDAO implements MetaDao<Study> {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public StudyDAO(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    //TODO: find a better home for this, temporary fix for findBugs
    UUID getIdKey(KeyHolder keyHolder) {
        Map<String, Object> keys = keyHolder.getKeys();
        if (keys != null) {
            Object id = keys.get("id");
            if (id != null) {
                return (UUID)id;
            }
        }
        return null;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Study study) {
        String sql = "INSERT INTO study (name, description) VALUES (:name, :description)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", study.getName());
        params.addValue("description", study.getDescription());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = getIdKey(keyHolder);
        createTables(studyId, study.getTables());
    }

    void createTables(UUID studyId, List<StudyTable> tables) {
        String sql = "INSERT INTO study_table (name, study_id) VALUES (:name, :study_id)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("study_id", studyId);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        for (StudyTable table : tables) {
            params.addValue("name", table.getName());
            jdbcTemplate.update(sql, params, keyHolder);
            UUID tableId = getIdKey(keyHolder);
            createColumns(tableId, table.getColumns());
        }
    }

    void createColumns(UUID tableId, List<StudyTableColumn> columns) {
        String sql = "INSERT INTO study_column (table_id, name, type) VALUES (:table_id, :name, :type)";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("table_id", tableId);
        for (StudyTableColumn column : columns) {
            params.addValue("name", column.getName());
            params.addValue("type", column.getType());
            jdbcTemplate.update(sql, params);
        }
    }

    @Override
    public Study retrieve(String id) {
        return null;
    }

    @Override
    public void delete(String id) {

    }

    @Override
    public List<Study> enumerate() {
        return null;
    }
}
