package bio.terra.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DataSnapshotTableDao extends TableDaoBase {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DataSnapshotTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        super(jdbcTemplate, "dataset_table", "dataset_column", "parent_id");
        this.jdbcTemplate = jdbcTemplate;
    }
}
