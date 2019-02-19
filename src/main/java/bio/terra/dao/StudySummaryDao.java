package bio.terra.dao;

import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class StudySummaryDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public StudySummaryDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public NamedParameterJdbcTemplate getJdbcTemplate() { return jdbcTemplate; }

    @Transactional
    public UUID create(Study study) {
        String sql = "INSERT INTO study (name, description, created_date) VALUES (:name, :description, :createdDate)";
        // TODO when new key holder is merged remove date code and get created date from key holder
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", study.getName())
                .addValue("description", study.getDescription())
                .addValue("createdDate", new Timestamp(now.toEpochMilli()));
        UUIDHolder keyHolder = new UUIDHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        UUID studyId = keyHolder.getId();
        study.setId(studyId);
        study.setCreatedDate(now);
        return studyId;
    }

    private static class StudySummaryMapper implements RowMapper<StudySummary> {
        public StudySummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new StudySummary()
                    .setId(UUID.fromString(rs.getString("id")))
                    .setName(rs.getString("name"))
                    .setDescription(rs.getString("description"))
                    .setCreatedDate(Instant.from(rs.getObject("created_date", OffsetDateTime.class)));
        }
    }

    public StudySummary retrieve(UUID id) throws StudyNotFoundException, EmptyResultDataAccessException {
        try {
            String sql = "SELECT id, name, description, created_date FROM study WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new StudySummaryMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new StudyNotFoundException("Study not found for id " + id.toString());
        }
    }

    // does not return sub-objects with studies
    public List<StudySummary> enumerate() {
        String sql = "SELECT, id, name, description, created_date FROM study";
        return jdbcTemplate.query(sql, new MapSqlParameterSource(), new StudySummaryMapper());
    }
}
