package bio.terra.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StudyTableDao extends TableDaoBase {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public StudyTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        super(jdbcTemplate, "study_table", "study_column", "study_id");
        this.jdbcTemplate = jdbcTemplate;
    }
}
